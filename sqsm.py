import uuid
import logging
import boto3


class Sqsm:
    def __init__(self, source_queue: str, target_queue: str, profile: str="default"):
        self.logger = logging.Logger("Sqsm")
        self.logger.addHandler(logging.StreamHandler())
        self._session = boto3.session.Session(profile_name=profile)
        self._client = self._session.client("sqs")
        self._source_queue = self._client.get_queue_url(QueueName=source_queue).get("QueueUrl")
        self._target_queue = self._client.get_queue_url(QueueName=target_queue).get("QueueUrl")

    def move_messages(self, delete: bool = False):
        while True:
            _messages = self._receive_messages(self._source_queue)
            if not _messages:
                break

            self._send_messages(self._target_queue, _messages)
            if delete:
                self._delete_messages(self._source_queue, _messages)

    def _receive_messages(self, queue_url: str) -> list:
        resp = self._client.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=10,
            WaitTimeSeconds=10
        )
        messages = resp.get("Messages", [])
        self.logger.debug("received {} message(s) from {}".format(len(messages), queue_url))
        return messages

    def _delete_messages(self, queue_url: str, messages):
        entries = [{"Id": message["MessageId"], "ReceiptHandle": message["ReceiptHandle"]} for message in messages]
        if len(entries) > 0:
            self.logger.debug("will remove {} message(s) from {}".format(len(entries), queue_url))
            self._client.delete_message_batch(QueueUrl=queue_url, Entries=entries)
        else:
            self.logger.debug("no message to remove from queue")

    def _send_messages(self, queue_url: str, messages: list):
        queue_type = "fifo" if queue_url.endswith(".fifo") else "standard"
        for message in messages:
            if queue_type == "fifo":
                unique_id = str(uuid.uuid4())
                self._client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=message["Body"],
                    MessageGroupId=unique_id,
                    MessageDeduplicationId=unique_id
                )
            else:
                self._client.send_message(
                    QueueUrl=queue_url,
                    MessageBody=message["Body"]
                )
        self.logger.debug("sent {} message to {}".format(len(messages), queue_url))
