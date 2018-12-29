# sqsm

Python3 script that moves SQS messages to another queue

It determines by looking .fifo suffix whether a queue is FIFO or standard.
Fifo or Standard -> Fifo or Standard

## Installation

    git clone https://github.com/safaozturk93/sqsm.git
    pip install -r requirements.txt


## Configuration

It reads AWS configuration profiles under ~/.aws/config.


## Usage

    from sqsm import Sqsm

    sqsm = Sqsm(
        source_queue="test-fo-dlq",
        target_queue="test-fo",
        profile="qa",
    )
    sqsm.move_messages(delete=False)
    
## License

The MIT License (MIT)

Copyright (c) 2018 safa ozturk

See [LICENSE.md](LICENSE.md)
