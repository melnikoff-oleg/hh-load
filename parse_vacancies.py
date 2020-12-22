from bs4 import BeautifulSoup
import codecs
import json
import pandas as pd
import re
import csv
import stanza
import nltk
from nltk.corpus import stopwords
from string import punctuation

#text cleaning
def clear_string(s):
    s = s.strip().replace('\n', '').replace('\r', '')
    s = re.sub('["«»;?!,()]', '', s)
    s = re.sub('[/—-]', ' ', s)
    s = re.sub(r"\\", ' ', s)
    s = re.sub('  ', ' ', s)
    rus_alphavite = 'АБВГДЕЁЖЗИЙКЛМНОПРСТУФХЦЧШЩЪЫЬЭЮЯ'
    for c in rus_alphavite:
        s = s.replace(c, c.lower())
    return s

def del_stopwords(doc):
    r = doc.copy()
    cur = 0
    for ind, i in enumerate(doc):
        if i['lemma'] in russian_stopwords or i['lemma'] in punctuation:
            r.pop(ind - cur)
            cur += 1
    return r

def doc_to_list(doc):
    doc_json = json.loads(str(doc))
    return doc_json[0]

def process_doc(doc):
    doc = doc_to_list(doc)
    doc = del_stopwords(doc)
    return doc

def get_lemms(doc):
    ans = []
    for i in doc:
        ans.append(i['lemma'])
    return ans

def check_eng(html_doc):
    a = re.search('[а-яА-Я]', html_doc)
    if a is None:
        return True
    return False


# main html processing func, parse vacancy on simple sections
def parse_html(vacancy_id, html_doc):
    if check_eng(html_doc):
        print("This vacancy is English")
        return []
    MAX_TITLE_LEN = 5
    MAX_BODY_LEN = 10
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
    result = []
    try:
        while current_p is not None and current_p.find_previous_sibling() is not None:
            next_p = current_p.find_previous_sibling()

            text = clear_string(next_p.text)
            if len(text) == 0:
                break
            words = get_lemms(process_doc(nlp(text)))
            length = min(MAX_BODY_LEN, len(words))
            body = ' '.join(words[: length])
            informal_part.append(body)
            current_p = next_p
        informal_part.reverse()
        for paragraph in informal_part:
            result.append([vacancy_id, 0, '', paragraph])
    except Exception as e:
        pass

    # process <strong> and next <ul>
    ptr = 1 if len(result) > 0 else 0
    for ul in soup.find_all('ul'):
        try:
            text = clear_string(ul.find_previous_sibling().text)
            words = get_lemms(process_doc(nlp(text)))
            length = min(MAX_TITLE_LEN, len(words))
            title = words[:length]
            title = ' '.join(title)
            items = [] 
            for li in ul.find_all('li'):
                s = clear_string(li.text)
                
                words = get_lemms(process_doc(nlp(s)))
                length = min(MAX_BODY_LEN, len(words))
                body = words[:length]
                body = ' '.join(body)
                items.append(body)

            for i in items:
                result.append([vacancy_id, ptr, title, i])
            ptr += 1
        except Exception as e:
            pass
    return result

if __name__ == '__main__':
    nltk.download("stopwords")
    russian_stopwords = stopwords.words("russian")
    nlp = stanza.Pipeline(lang='ru', processors='tokenize,pos,lemma')
    with codecs.open('vacancies.json', 'r', encoding='utf8') as f:
        vacancies = json.load(f)
        success = 0
        err_to_many_requests = 0
        err_no_tag = 0
        result = []
        bunch_size = 300
        lenght = min(bunch_size, len(vacancies))
        for ind, vacancy in enumerate(vacancies[: lenght]):
            try:
                print('{}/{}'.format(ind + 1, lenght))
                vacancy_id = vacancy['id']
                html_doc = vacancy['description']
                result += parse_html(vacancy_id, html_doc)
                success += 1
            except Exception as e:
                if str(e)[1: -1] == 'id':
                    err_to_many_requests += 1
                else:
                    print(e)
                    print(ind + 1)
                    err_no_tag += 1
                pass

    print('{}/{} vacancies successfully parsed.'.format(success, lenght))
    print('Too many requests err: {}'.format(err_to_many_requests))
    print('No tag err: {}'.format(err_no_tag))
    df = pd.DataFrame(result, columns=['vacancy_id', 'section_id', 'section_title', 'section_body'])
    df.to_csv('vacancies_parsed.csv', index=False, quoting=csv.QUOTE_NONE, escapechar=';')