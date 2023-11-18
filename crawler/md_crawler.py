from bs4 import BeautifulSoup
import requests
import g4f
from multiprocessing import Pool, Manager
from datetime import datetime   
from dotenv import load_dotenv
from os import getenv


GOV_URL = 'https://gov.md'
QUEUE = 'crawl'
NUMBER_OF_PROCESSES = 12
topics = ['Alegeri', 'Economie', 'Societate', 'Demonstratii', 'Politica']


def scanGovPage(url: str, headers) -> list:
    session = requests.session()
    response = session.get(url, headers=headers)
    if response.status_code == 200:
        parser = BeautifulSoup(response.content, features='html.parser')
        parser.prettify()

        news = []

        news_block = parser.find('div', class_='unformatted-list')
        for news_element in news_block.findChildren('div', class_='views-row'):
            info = {}

            title = news_element.findChild('div', class_='views-field views-field-title')
            header = title.findChild('span', class_='field-content').findChild('a', href=True)
            
            info['title'] = header.contents[0].strip()
            info['link'] = GOV_URL + header['href']

            content = news_element.findChild('div', class_='views-field views-field-body').findChild('div', class_='field-content')
            
            if content.findChild('div') != None:
                info['body'] = content.findChild('div').contents[0].strip()
            else:
                content_paragraphs = content.findChildren('p')

                spans = []
                for paragraph in content_paragraphs:
                    spans = paragraph.findChildren('span')
                    if spans != []:
                        break

                info['body'] = ''
                if len(spans) == 0:
                    info['body'] = content_paragraphs[0].contents[0].strip()
                else:
                    for span in spans:
                        info['body'] += span.contents[0].strip()

            info['timestamp'] = datetime.now().strftime("%H:%M:%S")
            news.append(info)

        return news
        
    else:
        print(f"Request failed: {response.status_code}")
        return []
    

def scanLocalPage(url: str) -> list:
    response = requests.get(url)
    if response.status_code == 200:
        parser = BeautifulSoup(response.content, features='html.parser')
        parser.prettify()

        news = []

        news_block = parser.find('div', class_='evo-post-wrap')
        for article in news_block.findChildren('article'):
            info = {}

            header = article.findChild(class_='evo-entry-title')
            link = header.findChild('a', href=True)
            
            info['title'] = link.contents[0].strip()
            info['link'] = link['href']

            content = article.findChild('div', class_='evo-entry-content')
            info['body'] = content.findChild('p').contents[0].strip()

            info['timestamp'] = datetime.now().strftime("%H:%M:%S")
            news.append(info)
        
        return news

    else:
        print(f"Request failed: {response.status_code}")
        return []


def detectTopics(topics: list, text: str):
    response = g4f.Completion.create(
        model='text-davinci-003',
        provider=g4f.Provider.Bing,
        prompt=f'Which topics from these {topics} best correspond to the following text: {text}'
    )

    result = []
    for topic in topics:
        if topic in response:
            result.append({
                'type': 1,
                'name': topic
            })

    return result


def processArticle(article):
    text = article['title'] + ' ' + article['body']
    t = detectTopics(topics, text)
    article['tags'] = t


def processWrapper(article):
    article_copy = article.copy()
    processArticle(article_copy)
    gov_news_shared.append(article_copy)


def main():
    gov_url = "https://gov.md/ro/comunicate-presa"
    newsmaker_url = "https://newsmaker.md/ro/politica/"
    headers = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    
    gov_news = scanGovPage(gov_url, headers)
    global gov_news_shared

    manager = Manager()
    gov_news_shared = manager.list()

    local_news = scanLocalPage(newsmaker_url)
    global local_news_shared
    local_news_shared = manager.list()

    with Pool(NUMBER_OF_PROCESSES) as pool:
        pool.map(processWrapper, gov_news)

    with Pool(NUMBER_OF_PROCESSES) as pool:
        pool.map(processWrapper, local_news)

    gov_articles = []
    for article in gov_news_shared:
        gov_articles.append(article)

    local_articles = []
    for article in local_news_shared:
        local_articles.append(article)

    news = {
        'posts': gov_articles + local_articles
    }

    headers = {
        'Content-Type': 'application/json',
        'X-Password': 'Djibouti',
    }

    load_dotenv()
    response = requests.post(getenv('API_ADDRESS'), headers=headers, json=news)
    if response.status_code == 200:
        print("Request successful!")
    else:
        print(f"Request failed with status code: {response.content}")
    
  
if __name__ == "__main__":
    main()