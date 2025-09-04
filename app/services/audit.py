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
            for i in range(0, len(fields), 2):
                obj[fields[i]] = fields[i+1]
            res.append(obj)
        return res
