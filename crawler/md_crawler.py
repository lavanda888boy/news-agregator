from bs4 import BeautifulSoup
import requests
import g4f
import pika
from multiprocessing import Pool


GOV_URL = 'https://gov.md'
QUEUE = 'crawl'
NUMBER_OF_PROCESSES = 8
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
            
            info['header'] = header.contents[0].strip()
            info['link'] = GOV_URL + header['href']

            content = news_element.findChild('div', class_='views-field views-field-body').findChild('div', class_='field-content')
            
            if content.findChild('div') != None:
                info['resume'] = content.findChild('div').contents[0].strip()
            else:
                content_paragraphs = content.findChildren('p')

                spans = []
                for paragraph in content_paragraphs:
                    spans = paragraph.findChildren('span')
                    if spans != []:
                        break

                info['resume'] = ''
                if len(spans) == 0:
                    info['resume'] = content_paragraphs[0].contents[0].strip()
                else:
                    for span in spans:
                        info['resume'] += span.contents[0].strip()

            info['status'] = 0
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
            
            info['header'] = link.contents[0].strip()
            info['link'] = link['href']

            content = article.findChild('div', class_='evo-entry-content')
            info['resume'] = content.findChild('p').contents[0].strip()

            info['status'] = 1
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
            result.append(topic)

    return result


def processArticle(article):
    text = article['header'] + ' ' + article['resume']
    t = detectTopics(topics, text)
    article['topics'] = t
    print(article)


def main():
    gov_url = "https://gov.md/ro/comunicate-presa"
    newsmaker_url = "https://newsmaker.md/ro/politica/"
    headers = {"User-Agent":"Mozilla/5.0 (X11; Linux x86_64; rv:60.0) Gecko/20100101 Firefox/60.0","Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"}
    
    gov_news = scanGovPage(gov_url, headers)
    local_news = scanLocalPage(newsmaker_url)
    
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE, durable=False)

    with Pool(NUMBER_OF_PROCESSES) as pool:
        pool.map(processArticle, gov_news)

    with Pool(NUMBER_OF_PROCESSES) as pool:
        pool.map(processArticle, local_news)
    
  
if __name__ == "__main__":
    main()