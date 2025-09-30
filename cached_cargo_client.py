import hashlib
import json
import os
import time


class CachedCargoClient:
    def __init__(self, cargo_client, cache_dir="query_cache", min_delay=1.0):
        self.client = cargo_client
        self.cache_dir = cache_dir
        os.makedirs(self.cache_dir, exist_ok=True)
        self.min_delay = min_delay
        self._last_query_time = 0

    def query(self, **kwargs):
        key_bytes = json.dumps(kwargs, sort_keys=True, default=str).encode("utf-8")
        cache_file = os.path.join(
            self.cache_dir, hashlib.md5(key_bytes).hexdigest() + ".json"
        )

        if os.path.exists(cache_file):
            with open(cache_file, "r", encoding="utf-8") as f:
                return json.load(f)

        now = time.time()
        elapsed = now - self._last_query_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)

        result = self.client.query(**kwargs)

        with open(cache_file, "w", encoding="utf-8") as f:
            json.dump(result, f, default=str)

        self._last_query_time = time.time()
        return result
