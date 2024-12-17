import unidecode

def clean_name(name):
    name = unidecode.unidecode(name)
    name = name.lower()
    name = f' {name} '
    name = name.replace(' sk ', '')
    name = name.replace(' fc ', '')
    name = name.replace(' bc ', '')
    name = name.replace(' bk ', '')
    name = name.replace(' as ', '')
    name = name.replace(' cs ', '')
    name = name.replace(' hc ', '')
    name = name.replace(' sp ', '')
    name = name.replace(' cb ', '')
    name = name.replace(' bc ', '')
    name = name.replace('  ', ' ')
    name = name.strip()
    return name

if __name__ == '__main__':
    print(clean_name('Bc Polkowice'))

