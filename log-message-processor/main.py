import time
import redis
import os
import json
import requests
import random

# Compatibility Wrapper for legacy thriftpy
try:
    import thriftpy2 as thriftpy
except ImportError:
    pass 

from py_zipkin.zipkin import zipkin_span, ZipkinAttrs, generate_random_64bit_string

def log_message(message):
    time_delay = random.randrange(0, 2000)
    time.sleep(time_delay / 1000)
    print('message received after waiting for {}ms: {}'.format(time_delay, message))

if __name__ == '__main__':
    # Environment Variables
    redis_host = os.environ.get('REDIS_HOST', 'localhost')
    redis_port = int(os.environ.get('REDIS_PORT', 6379))
    redis_channel = os.environ.get('REDIS_CHANNEL', 'log_channel')
    zipkin_url = os.environ.get('ZIPKIN_URL', '')

    def http_transport(encoded_span):
        requests.post(
            zipkin_url,
            data=encoded_span,
            headers={'Content-Type': 'application/x-thrift'},
        )

    # Redis Connection
    r = redis.Redis(host=redis_host, port=redis_port, db=0)
    pubsub = r.pubsub()
    pubsub.subscribe([redis_channel])

    print(f"Subscribed to {redis_channel}. Waiting for messages...")

    for item in pubsub.listen():
        if item['type'] != 'message':
            continue

        try:
            message = json.loads(item['data'].decode("utf-8"))
        except Exception as e:
            log_message(f"Error decoding JSON: {e}")
            continue

        # Process without Zipkin if URL is missing
        if not zipkin_url or 'zipkinSpan' not in message:
            log_message(message)
            continue

        span_data = message['zipkinSpan']
        try:
            with zipkin_span(
                service_name='log-message-processor',
                zipkin_attrs=ZipkinAttrs(
                    trace_id=span_data['_traceId']['value'],
                    span_id=generate_random_64bit_string(),
                    parent_span_id=span_data['_spanId'],
                    is_sampled=span_data['_sampled']['value'],
                    flags=None
                ),
                span_name='save_log',
                transport_handler=http_transport,
                sample_rate=100
            ):
                log_message(message)
        except Exception as e:
            print('did not send data to Zipkin: {}'.format(e))
            log_message(message)
