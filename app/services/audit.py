from typing import Dict, Any, List
from ..utils.extensions import redis_cli, jset


class AuditService:
    @staticmethod
    def list(limit: int = 100):
        r = redis_cli.r
        stream = 'cm:stream:audit'
        # XREVRANGE stream + - COUNT N
        entries = r.execute_command('XREVRANGE', stream, '+', '-', 'COUNT', str(limit)) or []
        res = []
        for sid, fields in entries:
            # fields is list like [k1,v1,k2,v2,...]
            obj = {"id": sid}
            try:
                if isinstance(fields, dict):
                    for k, v in fields.items():
                        obj[str(k)] = v
                elif isinstance(fields, (list, tuple)):
                    # could be flat [k1,v1,k2,v2] or list of pairs [[k,v],...]
                    if fields and isinstance(fields[0], (list, tuple)) and len(fields[0]) == 2:
                        for k, v in fields:
                            obj[str(k)] = v
                    else:
                        for i in range(0, len(fields), 2):
                            k = fields[i]
                            v = fields[i+1] if i+1 < len(fields) else None
                            obj[str(k)] = v
                else:
                    # unknown shape; best-effort stringify
                    obj['raw'] = str(fields)
            except Exception:
                obj['raw'] = str(fields)
            res.append(obj)
        return res
