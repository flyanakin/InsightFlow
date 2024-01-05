import os
import requests
import json

wechat_token = os.getenv('WECOM_BOT_TOKEN_KIKYO')


def send_wework_message(message: str):
    hook_url = f"https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key={wechat_token}"
    post_data = {"msgtype": "text", "text": {"content": message}}

    headers = {"Content-Type": "application/json"}
    requests.post(
        url=hook_url,
        headers=headers,
        data=json.dumps(post_data),
    )