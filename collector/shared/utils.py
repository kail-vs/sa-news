import hashlib

def extract_ids(items):
    ids = []
    for item in items:
        raw = str(item).encode("utf-8")
        ids.append(hashlib.md5(raw).hexdigest())
    return ids

def overlap_ratio(new_ids, old_ids):
    if not old_ids:
        return 0.0
    overlap = len(set(new_ids) & set(old_ids))
    return overlap / max(len(new_ids), 1)
