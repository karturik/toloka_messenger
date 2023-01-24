from bs4 import BeautifulSoup
import pandas as pd
import requests
import toloka.client as toloka
from tqdm import tqdm
from deep_translator import GoogleTranslator, single_detection
import json


URL_WORKER = 'https://toloka.yandex.ru/requester/worker/'
URL_API = "https://toloka.yandex.ru/api/v1/"
OAUTH_TOKEN = ''
HEADERS = {"Authorization": "OAuth %s" % OAUTH_TOKEN, "Content-Type": "application/JSON"}
toloka_client = toloka.TolokaClient(OAUTH_TOKEN, 'PRODUCTION')

translator_api_key = ''

messages = requests.get(f'https://toloka.dev/api/v1/message-threads?folder=UNREAD', headers=HEADERS).json()

full_df = pd.DataFrame()

# CREATE EXCEL FILE WITH ALL UNREAD MESSAGES
for i in tqdm(messages['items']):
    tries = 0
    success = False
    while success != True:
        try:
            message_data = {}
            messages_texts = []
            for message in i['messages']:
                message_text = list(message['text'].values())[0]
                message_text = BeautifulSoup(message_text, 'html.parser').text.replace('\n', ' ')
                messages_texts.append(message_text)
            message_data['message_id'] = i['id']
            message_data['worker_id'] = i['messages'][0]['from']['id']
            if i['meta']:
                message_data['assignment_link'] = f"https://toloka.yandex.ru/en/requester/project/{i['meta']['project_id']}/pool/{i['meta']['pool_id']}/assignments/{i['meta']['assignment_id']}?direction=ASC"
                message_data['project_name'] = requests.get(f"https://toloka.dev/api/v1/projects/{i['meta']['project_id']}", headers=HEADERS).json()['public_name']
            else:
                message_data['assignment_link'] = ""
                message_data['project_name'] = ""
            message_data['worker_language'] = [single_detection(BeautifulSoup(list(i['messages'][0]['text'].values())[0], 'html.parser').text.replace('\n', ' '), api_key=translator_api_key)]
            message_data['message_full_chat'] = ' ❌ '.join(messages_texts)
            message_data['message_full_chat_russian'] = GoogleTranslator(source='auto', target='ru').translate(' ❌ '.join(messages_texts))
            message_data['message_text_last_message'] = BeautifulSoup(list(i['messages'][0]['text'].values())[0], 'html.parser').text.replace('\n', ' ')
            message_data['message_text_last_message_russian'] = GoogleTranslator(source='auto', target='ru').translate(BeautifulSoup(list(i['messages'][0]['text'].values())[0], 'html.parser').text.replace('\n', ' '))
            message_data['answer_russian'] = ''
            df = pd.DataFrame(data=message_data)
            message_data = {}

            full_df = pd.concat([full_df, df])
            success = True

        except Exception as e:
            print('Error ', e)
            print('Try ', tries, '/10')
            tries += 1
            if tries == 10:
                success = True



full_df.to_excel('messenger.xlsx', index=False)

for i in messages['items']:
    messages_texts = []
    for message in i['messages']:
        # print(message['text'])
        # print(message['text'].values())
        message_text = list(message['text'].values())[0]
        message_text = BeautifulSoup(message_text, 'html.parser').text.replace('\n', ' ')
        messages_texts.append(message_text)
    print(' 🛑 '.join(messages_texts))


# SECOND CELL FOR ANSWWER ON MESSAGES
messenger_df = pd.read_excel('messenger.xlsx', sheet_name='Sheet1')

messenger_df = messenger_df.dropna(subset=['answer_russian'])

for message_id in messenger_df['message_id']:
    print('Сообщение: ', message_id)

    answer = messenger_df[messenger_df['message_id']==message_id]['answer_russian'].values[0]
    answer_language = messenger_df[messenger_df['message_id']==message_id]['worker_language'].values[0]
    if '-' in answer_language: answer_language = answer_language.split('-')[0]
    if answer != '+':
        message_body = {
            "text": {
                "EN": GoogleTranslator(source='auto', target=answer_language).translate(answer),
                "RU": answer
            }
        }
        r = requests.post(url=f'https://toloka.dev/api/v1/message-threads/{message_id}/reply', headers=HEADERS, data=json.dumps(message_body))
        print('Answered on message')

    message_remove_folders = {
        "folders": ["UNREAD"]
    }
    requests.post(url=f'https://toloka.dev/api/v1/message-threads/{message_id}/remove-from-folders', headers=HEADERS, data=json.dumps(message_remove_folders))
    print('Move message')

print(messenger_df)

