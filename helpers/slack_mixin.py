import os
import json
import typing as t

import requests


class SlackMixin:
    def __init__(
        self,
        webhook_url: t.Optional[str] = None,
        project_name: t.Optional[str] = None,
    ) -> None:
        if webhook_url:
            self._webhook = webhook_url
        else:
            hook = os.environ.get("SLACK_URL")
            if not hook:
                raise Exception(
                    "Provide SLACK_URL env variable or pass directly"
                )
            self._webhook = hook
        self._project = project_name

    def slack_msg(self, msg: str) -> None:
        if not msg:
            return
        slack_data = {
            "text": f"{self._project} | {msg}" if self._project else msg
        }
        response = requests.post(
            url=self._webhook,
            data=json.dumps(slack_data),
            headers={"Content-Type": "application/json"},
        )
        if response.status_code != 200:
            raise ValueError(
                f"Request to the slack server is unsuccessful."
                f"Error: {response.status_code}, "
                f"Response: {response.text}"
            )
        return
