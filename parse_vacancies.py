from bs4 import BeautifulSoup
import codecs
import json
import pandas as pd

def parse_html(vacancy_id, html_doc):
    MIN_BEGINNING_PART = 3
    soup = BeautifulSoup(html_doc, 'html.parser')

    # process beginning part before <strong>
    first_ul = soup.ul
    try:
        first_strong = first_ul.find_previous_sibling('p')
    except Exception as e:
        first_strong = soup.find_all('p')[-1]
    if first_strong is None:
        first_strong = first_ul.find_previous_sibling('strong')
    informal_part = []
    current_p = first_strong
    try:
        while current_p is not None and current_p.find_previous_sibling('p') is not None:
            next_p = current_p.find_previous_sibling('p')
            words = next_p.text.split(' ')
            length = min(MIN_BEGINNING_PART, len(words))
            s = ' '.join(words[: length])
            if length < len(words):
                s += '...'
            informal_part.append(s)
            current_p = next_p
        informal_part.reverse()
        result = []
        for paragraph in informal_part:
            result.append([vacancy_id, 0, '', paragraph])
    except Exception as e:
        pass

    # process <strong> and next <ul>
    ptr = 1 if len(result) > 0 else 0
    for ul in soup.find_all('ul'):
        try:
            title = ul.find_previous_sibling('p').text
            if title[-1] == ':':
                title = title[:-1]
            items = [] 
            for li in ul.find_all('li'):
                s = li.text.strip().replace('\n', '').replace('\r', '')
                t = ''
                space = False
                for char in s:
                    if char == ' ' and space:
                        continue
                    t += char
                    space = (char == ' ')
                items.append(t)

            for i in items:
                result.append([vacancy_id, ptr, title, i])
            ptr += 1
        except Exception as e:
            pass
    
    return result
if __name__ == '__main__':
    with codecs.open('vacancies.json', 'r', encoding='utf8') as f:
        vacancies = json.load(f)
        success = 0
        err_to_many_requests = 0
        err_no_tag = 0
        result = []
        bunch_size = 5000
        lenght = min(bunch_size, len(vacancies))
        for vacancy in vacancies[: lenght]:
            try:
                vacancy_id = vacancy['id']
                html_doc = vacancy['description']
                result += parse_html(vacancy_id, html_doc)
                success += 1
            except Exception as e:
                if str(e)[1: -1] == 'id':
                    err_to_many_requests += 1
                else:
                    err_no_tag += 1
                pass

    print('{}/{} vacancies successfully parsed.'.format(success, lenght))
    print('Too many requests err: {}'.format(err_to_many_requests))
    print('No tag err: {}'.format(err_no_tag))
    df = pd.DataFrame(result, columns=['vacancy_id', 'section_id', 'section_title', 'section_body'])
    df.to_csv('vacancies_parsed.csv', index=False)