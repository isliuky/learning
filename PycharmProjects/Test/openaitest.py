import os
import openai

openai.api_type = "azure"
openai.api_base = "https://novartars.openai.azure.com/"
openai.api_version = "2023-07-01-preview"
openai.api_key = '27a9e55e4c7e4937a0a1b7850d3dc2e0'

message_text = [{"role":"system","content":"You are an AI assistant that helps people find information."}]

completion = openai.Completion.create(
  engine="gpt35turbo",
  messages = message_text,
  temperature=0.7,
  max_tokens=800,
  top_p=0.95,
  frequency_penalty=0,
  presence_penalty=0,
  stop=None
)
print(completion)
for message in completion.messages:
  if message["type"] == "text":
    print(message["text"])

#
# import requests
# import json
# import os
#
# # Azure 认证信息
# subscription_key = "27a9e55e4c7e4937a0a1b7850d3dc2e0"
# endpoint = "https://novartars.openai.azure.com/openai/deployments/gpt35turbo/chat/completions?api-version=2023-07-01-preview"
#
# # OpenAI API URL
# openai_url = 'https://api.openai.com/v1/engines/davinci-codex/completions'
#
# # 请求头信息
# headers = {
#     'Content-Type': 'application/json',
#     'Authorization': 'Bearer ' + subscription_key
# }
#
# # 请求体信息
# payload = {
#     'prompt': 'How to write Python code to access Azure OpenAI?',
#     'max_tokens': 5
# }
#
# # 发送 POST 请求，获取 OpenAI API 的响应
# response = requests.post(openai_url, headers=headers, data=json.dumps(payload))
#
# # 解析响应数据
# if response.status_code == 200:
#     data = response.json()
#     text = data['choices'][0]['text']
#     print(text)
# else:
#     print('Error:', response.status_code, response.text)
