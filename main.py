import asyncio
import json
import time
from rapidfuzz import process, fuzz

from httpx import ReadTimeout
from requests.structures import CaseInsensitiveDict

from classes import Pinnacle, Betonline
from tools.devig import worst_case_amer, dec_to_amer, calculate_ev, worst_case_amer_to_dec
from sending import send_graph
import logging


def sort_dicts_by_key(dict_list, key):
    # Use the sorted function and specify the key to sort by 'ev' value in each dictionary
    sorted_list = sorted(dict_list, key=lambda x: x[key], reverse=True)
    return sorted_list


def is_convertible_to_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


def format_fv(fv, r=True):
    fv = float(fv)
    if r:
        fv = int(fv)
    else:
        fv = round(fv, 1)
    if fv > 0:
        return f"+{fv}"
    else:
        return f"{fv}"


Classes = Pinnacle, Betonline

period_dict = {
    'basketball': ['full'],

}

books = []
periods = []


async def timed_task(task, *args, **kwargs):
    start_time = time.time()
    result = await task(*args, **kwargs)
    end_time = time.time()
    duration = end_time - start_time
    return result, duration


async def match(books, old_names, live=True):

    tasks = [timed_task(book.get_events_data, live=live) for book in books]
    results = await asyncio.gather(*tasks)

    books_data = []
    for (result, duration), book in zip(results, books):
        print(f"Task {book.name} took {duration:.2f} seconds")
        # Add the result and book name to books_data
        books_data.append((result, book.name))
    pin_teams = set()

    for data, book_name in books_data:
        if not data:
            continue
        for game_name, game_data in data.items():
            teams = game_name.split(' @ ')
            for team in teams:
                if book_name == 'pin':
                    pin_teams.add(team)
    for data, book_name in books_data:
        for game_name, game_data in data.items():
            teams = game_name.split(' @ ')
            for team in teams:
                if team not in pin_teams and team not in old_names:
                    choices = process.extract(team, pin_teams, limit=5, score_cutoff=50, scorer=fuzz.token_set_ratio)
                    for closest_match, confidence, _ in choices:
                        user_input = input(f"{team} / {closest_match}: {confidence:.2f}%: ")
                        if user_input == '1':
                            old_names[team] = closest_match
                            break

    return old_names


async def run_match(sport, live):
    with open('jsons/team_names.json', 'r') as f:
        basketball_names = json.load(f)
    for Class in Classes:
        books.append(Class(sport))
    periods = period_dict.get(sport)
    t = time.time()
    data = await match(books, basketball_names, live=live)
    if not data:
        return
    with open(f'jsons/new_name_map.json', 'w') as f:
        json.dump(data, f, indent=4)


class Datafetcher:
    def __init__(self, sport, live):
        self.sport = sport
        self.live = live
        self.books = [cls(sport) for cls in Classes]
        self.periods = period_dict.get(sport)
        self.markets = ['total', 'Money Line', 'spread', '3-way']
        self.pin_data = {}

    async def run(self):
        tasks = [timed_task(book.get_events_data, live=self.live) for book in self.books]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logging.error(f"Error fetching data from {self.books[i].name} - {result}")

        books_data = []
        results = [result for result in results if not isinstance(result, Exception)]
        for (result, duration), book in zip(results, self.books):
            print(f"Task {book.name} took {duration:.2f} seconds")
            # Add the result and book name to books_data
            books_data.append((result, book.name))
        view = {}

        for period in self.periods:
            for market in self.markets:
                for data, book_name in books_data:
                    if not data:
                        continue
                    for game_name, game_data in data.items():
                        game_name = game_name.title()
                        try:
                            away_team, home_team = game_name.split(' @ ')
                        except:
                            continue
                        with open('jsons/team_names.json', 'r') as f:
                            team_names = json.load(f)
                            team_names = CaseInsensitiveDict(team_names)
                        home_team, away_team = team_names.get(home_team, home_team), team_names.get(away_team, away_team)
                        game_name = f"{away_team} @ {home_team}".title()
                        period_market_data = game_data.get(period, {}).get(market, {})
                        if not period_market_data:
                            period_market_data = game_data.get('odds', {}).get(period, {}).get(market, {})
                            if not period_market_data:
                                continue

                        game_info = game_data.get('info', {})
                        game_date, game_league = game_info.get('date'), game_info.get('league')

                        is_timeout = game_info.get('is_timeout', False)

                        dk_timeout = game_info.get('dk_timeout', False)

                        row = {
                            'book': book_name,
                            'is_timeout': is_timeout,
                            'data': period_market_data,
                            'dk_timeout': dk_timeout,
                            'score': game_info.get('score', None),
                        }



                        view.setdefault(game_name, {}).setdefault(period, {}).setdefault(market, []).append(row)
                        if book_name == 'pin':
                            view[game_name]['info'] = game_info
        return view

    @staticmethod
    def find_ev(data, sport, sharp_name='pin', need_timeout=False, dk_timeout=False, fallback_sharp=None, ev_threshold=5,
                spread_threshold=1, total_threshold=1.5, half_threshold=1):
        rows = []
        ld_rows = []
        ev_threshold = float(ev_threshold)

        def get_ld_threshold(period, market):
            if period == 'full':
                if market == 'spread':
                    return spread_threshold
                else:
                    return total_threshold
            return half_threshold

        def get_sharp_row(market_data):
            for row in market_data:
                if row['book'] == sharp_name:
                    return row, sharp_name

            if fallback_sharp:
                for row in market_data:
                    if row['book'] == fallback_sharp:
                        return row, fallback_sharp

            return None, None

        def add_bet_info(bet, name, fair, num, home_team, away_team, period, row, link, odds, market, sharp_name, score, limit):
            if market == 'total':
                bet_desc = f'o{num}' if name == 'one' else f'u{num}'
            else:
                bet_desc = f'{home_team} {format_fv(num, False)}' if name == 'one' else f'{away_team} {format_fv(-float(num), False)}'

            if bet[0] > ev_threshold:
                rows.append({
                    'book': row['book'],
                    'odds': format_fv(odds),
                    'link': link,
                    'ev': round(bet[0], 1),
                    'qk': round(bet[1], 2),
                    'bet': f"{period if period != 'full' else ''} {bet_desc}",
                    'fair': format_fv(fair),
                    'sharp': sharp_name,
                    'game': f'{away_team} @ {home_team}{f' {score}' if score else ""}',
                    'sport': sport,
                    'game_info': game_info,
                    'limit': limit,
                    'market': market,
                    'num': num
                })

        def process_market_data(market_data, sharp_data, game, period, market, score, sharp_name, game_info):
            away_team, home_team = game.split(' @ ')
            for row in market_data:
                if row['book'] == sharp_name or row['book'] in ['pin']:
                    continue


                book_name = row['book']
                if sharp_name != 'pin':
                    if book_name != 'fliff':
                        continue
                row_data = row['data']
                for bet_name, bet_data in row_data.items():
                    if market == '3-way':
                        sides = ['one', 'two', 'three']
                        sides.remove(bet_name)
                        if not sharp_data[bet_name] or not sharp_data[sides[0]] or not sharp_data[sides[1]]:
                            continue

                        fair = worst_case_amer([sharp_data[bet_name], sharp_data[sides[0]], sharp_data[sides[1]]])
                        if isinstance(bet_data, list):
                            odds, link = bet_data
                        elif isinstance(bet_data, dict):
                            odds, link = bet_data['odds'], bet_data['link']
                        else:
                            odds, link = bet_data, None
                        ev, qk = calculate_ev(odds, fair)
                        if not ev:
                            continue
                        if bet_name == 'one':
                            bet = home_team
                        elif bet_name == 'two':
                            bet = away_team
                        else:
                            bet = 'draw'
                        if ev > ev_threshold:
                            rows.append({
                                'book': row['book'],
                                'odds': format_fv(odds),
                                'link': link,
                                'ev': round(ev, 1),
                                'qk': round(qk, 2),
                                'bet': f"{period if period != 'full' else ''} {bet} 3-Way ML",
                                'fair': format_fv(fair),
                                'sharp': sharp_name,
                                'game': f'{away_team} @ {home_team} {score if score else ""}',
                                'sport': sport,
                                'game_info': game_info,
                                'limit': sharp_dataget('limit', None)
                            })
                    else:

                        if is_convertible_to_float(bet_name):  # if market is total or spread
                            num = float(bet_name)
                            if num not in sharp_data:
                                num = str(num)
                                if num not in sharp_data:
                                    num=float(num)

                            if num in sharp_data:
                                sharp_bet_one = sharp_data[num].get('one')
                                sharp_bet_two = sharp_data[num].get('two')
                                limit = sharp_data[num].get('max', None)
                                if isinstance(sharp_bet_one, list):
                                    sharp_bet_one = sharp_bet_one[0]
                                if isinstance(sharp_bet_two, list):
                                    sharp_bet_two = sharp_bet_two[0]
                                if isinstance(sharp_bet_one, dict):
                                    sharp_bet_one = sharp_bet_one['odds']
                                if isinstance(sharp_bet_two, dict):
                                    sharp_bet_two = sharp_bet_two['odds']
                                bet_one_rec, bet_two_rec = bet_data.get('one'), bet_data.get('two')
                                bet_one_fair = worst_case_amer([sharp_bet_one, sharp_bet_two])
                                if isinstance(bet_one_rec, list):
                                    bet_one_odds, bet_one_link = bet_one_rec
                                elif isinstance(bet_one_rec, dict):
                                    bet_one_odds, bet_one_link = bet_one_rec['odds'], bet_one_rec['link']
                                else:
                                    bet_one_odds, bet_one_link = bet_one_rec, None
                                if isinstance(bet_two_rec, list):
                                    bet_two_odds, bet_two_link = bet_two_rec
                                elif isinstance(bet_two_rec, dict):
                                    bet_two_odds, bet_two_link = bet_two_rec['odds'], bet_two_rec['link']
                                else:
                                    bet_two_odds, bet_two_link = bet_two_rec, None

                                if bet_one_odds and bet_one_odds != 'N/A':
                                    bet_one_info = calculate_ev(bet_one_odds, bet_one_fair)
                                    add_bet_info(bet_one_info, 'one', bet_one_fair, num, home_team, away_team, period,
                                                 row,
                                                 bet_one_link, bet_one_odds, market_name, sharp_name, score, limit)

                                if bet_two_odds and bet_two_odds != 'N/A':
                                    bet_two_fair = worst_case_amer([sharp_bet_two, sharp_bet_one])
                                    bet_two_info = calculate_ev(bet_two_odds, bet_two_fair)
                                    add_bet_info(bet_two_info, 'two', bet_two_fair, num, home_team, away_team, period,
                                                 row,
                                                 bet_two_link,
                                                 bet_two_odds, market_name, sharp_name, score, limit)
                            else:
                                sharp_keys = sorted([float(key) for key in sharp_data.keys()])
                                closest_number = min(sharp_keys, key=lambda x: abs(x - float(num)))
                                limit = sharp_data.get(closest_number, {}).get('max', None)

                                if market == 'total':
                                    difference = float(num) - closest_number
                                    if difference > 0:
                                        side = 'two', 'one'
                                        second_closest_number = closest_number - 0.5
                                        bet_desc = f'u{num}'

                                        def calculate_slope(closest_imp, second_imp):
                                            return (closest_imp - second_imp) * 2
                                    else:
                                        side = 'one', 'two'
                                        second_closest_number = closest_number + 0.5
                                        bet_desc = f'o{num}'

                                        def calculate_slope(closest_imp, second_imp):
                                            return (second_imp - closest_imp) * 2
                                else:
                                    if (num > 0) != (closest_number > 0):
                                        continue
                                    difference = float(num) - closest_number
                                    if difference > 0:
                                        side = 'one', 'two'
                                        second_closest_number = closest_number - 0.5
                                        bet_desc = f'{home_team} {format_fv(num, False)}'

                                        def calculate_slope(closest_imp, second_imp):
                                            return (closest_imp - second_imp) * 2
                                    else:
                                        side = 'two', 'one'
                                        second_closest_number = closest_number + 0.5
                                        bet_desc = f'{away_team} {(format_fv((-float(num)), False))}'

                                        def calculate_slope(closest_imp, second_imp):
                                            return (second_imp - closest_imp) * 2
                                sharp_closest = sharp_data.get(closest_number)
                                if sport == 'basketball':

                                    sharp_second_closest = sharp_data.get(second_closest_number)
                                    if sharp_second_closest:
                                        closest_fair = float(
                                            worst_case_amer_to_dec([sharp_closest[side[0]], sharp_closest[side[1]]]))
                                        closest_imp = 1 / closest_fair
                                        second_closest_fair = float(
                                            worst_case_amer_to_dec([sharp_second_closest[side[0]], sharp_second_closest[side[1]]]))
                                        second_closest_imp = 1 / second_closest_fair
                                        slope = calculate_slope(closest_imp, second_closest_imp)
                                        boost = difference * slope
                                        bet_imp = boost + closest_imp
                                        if sport != 'basketball':
                                            bet_imp = closest_imp
                                        fair_american = dec_to_amer(1 / bet_imp)
                                        bet_info = bet_data.get(side[0], None)
                                        if isinstance(bet_info, list):
                                            odds, link = bet_info
                                        elif isinstance(bet_info, dict):
                                            odds, link = bet_info['odds'], bet_info['link']
                                        else:
                                            odds, link = bet_info, None
                                        if odds is None:
                                            continue
                                        if isinstance(bet_info, dict) and 'alternate' in bet_info.get('market', '').lower():
                                            if odds < -120:
                                                continue
                                        if odds < -150:
                                            continue
                                        bet_info = calculate_ev(odds, fair_american)
                                        if bet_info[0] > ev_threshold:
                                            rows.append({
                                                'book': row['book'],
                                                'odds': format_fv(odds),
                                                'link': link,
                                                'ev': round(bet_info[0], 1),
                                                'qk': round(bet_info[1], 2),
                                                'bet': f"{period if period != 'full' else ''} {bet_desc}",
                                                'fair': format_fv(fair_american),
                                                'sharp': sharp_name,
                                                'game': f'{away_team} @ {home_team} {score if score else ""}',
                                                'ld': 'ext',
                                                'sport': sport,
                                                'game_info': game_info,
                                                'limit': limit,
                                                'market': market,
                                                'num': num
                                            })
                                else:
                                    if not sharp_closest:
                                        continue
                                    fair_american = worst_case_amer([sharp_closest[side[0]], sharp_closest[side[1]]])
                                    bet_info = bet_data.get(side[0], None)
                                    if isinstance(bet_info, list):
                                        odds, link = bet_info
                                    elif isinstance(bet_info, dict):
                                        odds, link = bet_info['odds'], bet_info['link']
                                    else:
                                        odds, link = bet_info, None
                                    if odds is None:
                                        continue
                                    if isinstance(bet_info, dict) and 'alternate' in bet_info.get('market', '').lower():
                                        if odds < -120:
                                            continue
                                    bet_info = calculate_ev(odds, fair_american)
                                    if bet_info[0] > ev_threshold:
                                        rows.append({
                                            'book': row['book'],
                                            'odds': format_fv(odds),
                                            'link': link,
                                            'ev': round(bet_info[0], 1),
                                            'qk': round(bet_info[1], 2),
                                            'bet': f"{period if period != 'full' else ''} {bet_desc}",
                                            'fair': format_fv(fair_american),
                                            'sharp': sharp_name,
                                            'game': f'{away_team} @ {home_team} {score if score else ""}',
                                            'ld': 'calc',
                                            'sport': sport,
                                            'game_info': game_info,
                                            'limit': limit,
                                            'market': market,
                                            'num': num
                                        })



                        else:  # moneyline
                            side_2 = 'home' if bet_name == 'away' else 'away'
                            limit = sharp_data.get('bet_name', {}).get('max', None)
                            try:
                                fair = worst_case_amer([sharp_data[bet_name], sharp_data[side_2]])
                            except:
                                try:
                                    fair = worst_case_amer([sharp_data[bet_name][0], sharp_data[side_2][0]])
                                except:
                                    continue
                            if isinstance(bet_data, list):
                                odds, link = bet_data
                            elif isinstance(bet_data, dict):
                                odds, link = bet_data['odds'], bet_data['link']
                            else:
                                odds, link = bet_data, None
                            ev, qk = calculate_ev(odds, fair)
                            if not ev:
                                continue
                            if ev > ev_threshold:
                                rows.append({
                                    'book': row['book'],
                                    'odds': format_fv(odds),
                                    'link': link,
                                    'ev': round(ev, 1),
                                    'qk': round(qk, 2),
                                    'bet': f"{period if period != 'full' else ''} {home_team if bet_name == 'home' else away_team} ML",
                                    'fair': format_fv(fair),
                                    'sharp': sharp_name,
                                    'game': f'{away_team} @ {home_team} {score if score else ""}',
                                    'sport': sport,
                                    'game_info': game_info,
                                    'limit': limit,
                                    'market': market,
                                    'num': bet_name
                                })

        for game, game_data in data.items():
            game_info = game_data.get('info', {})
            if 'link' in game_data:
                game_data = game_data['odds']
            for period, period_data in game_data.items():
                if period == 'info':
                    continue
                for market_name, market_data in period_data.items():
                    score = None
                    for row in market_data:
                        if row.get('score'):
                            score = row['score']
                            break
                    timeout = False
                    if dk_timeout:
                        if any(row.get('dk_timeout') == True for row in market_data):
                            print(f'{game} {period} {market_name} has a timeout')
                            timeout=True

                    if not timeout and need_timeout and not any(row['is_timeout'] for row in market_data) and sport == 'basketball':
                        continue
                    sharp_row, sharp = get_sharp_row(market_data)
                    if not sharp_row or not sharp_row.get('data'):
                        continue

                    process_market_data(market_data, sharp_row['data'], game, period, market_name, score, sharp, game_info)

        return sort_dicts_by_key(rows, 'ev'), sort_dicts_by_key(ld_rows, 'diff')




async def main():
    d = Datafetcher('basketball', live=False)
    p = Pinnacle('basketball')
    while True:
        with open('pings.json', 'r') as f:
            old_pings = json.load(f)
        data = await d.run()
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=4)

        ev, ld = Datafetcher.find_ev(data, 'basketball', sharp_name='pin', need_timeout=False, ev_threshold=-100,
                                     spread_threshold=1.5, total_threshold=1.5, half_threshold=1.5)
        for e in ev:

            if e['ev'] > -10:
                print(e)
                history = p.get_odds_history(e['game_info']['sql_key'], e['market'], f'full:{e["market"]}:{e["num"]}')
                history.reverse()
                send_graph(history, f'{e["bet"]} {e["odds"]}', f'{e["game"]}:{e["market"]}:{e["num"]}',
                            f'ev: {e["ev"]} qk: {e["qk"]} max: {e["limit"]}', e['game'])
        await asyncio.sleep(60)




if __name__ == '__main__':
    asyncio.run(main())
