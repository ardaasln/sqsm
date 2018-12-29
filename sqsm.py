import uuid
import logging
import boto3


class Sqsm:
    def __init__(self, source_queue: str, target_queue: str, profile: str = "default"):
        self._logger = logging.Logger("Sqsm")
        self._logger.addHandler(logging.StreamHandler())
        self._session = boto3.session.Session(profile_name=profile)
        self._client = self._session.client("sqs")
        self._source_queue = self._client.get_queue_url(QueueName=source_queue).get(
            "QueueUrl"
        )
        self._target_queue = self._client.get_queue_url(QueueName=target_queue).get(
            "QueueUrl"
        )

    def move_messages(self, delete: bool = False):
        while True:
            _messages = self._receive_messages(self._source_queue)
            if not _messages:
                break

            self._send_messages(self._target_queue, _messages)
            if delete:
                self._delete_messages(self._source_queue, _messages)

    def _receive_messages(self, queue_url: str) -> list:
        _resp = self._client.receive_message(
            QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=10
        )
        messages = _resp.get("Messages", [])
        self._logger.debug(
            "received {} message(s) from {}".format(len(messages), queue_url)
        )
        return messages

    def _delete_messages(self, queue_url: str, messages):
        entries = [
            {"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]}
            for message in messages
        ]
        if len(entries) > 0:
            self._logger.debug(
                "will remove {} message(s) from {}".format(len(entries), queue_url)
            )
            self._client.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        else:
            self._logger.debug("no message to remove from queue")

    def _send_messages(self, queue_url: str, messages: list):
        _queue_type = "fifo" if queue_url.endswith(".fifo") else "standard"
        for _message in messages:
            if _queue_type == "fifo":
                unique_id = str(uuid.uuid4())
                self._client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=_message["Body"],
                    MessageGroupId=unique_id,
                    MessageDeduplicationId=unique_id,
                )
            else:
                self._client.send_message(
                    QueueUrl=queue_url, MessageBody=_message["Body"]
                )
        self._logger.debug("sent {} message to {}".format(len(messages), queue_url))
