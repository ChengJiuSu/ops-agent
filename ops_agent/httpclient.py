"""标准库 urllib 的极简 HTTP 客户端：Jenkins / JumpServer 等 REST 调用共用。

agent 部署只依赖 websockets + PyYAML，不引入 requests，故自己封一层：

- `ProxyHandler({})` 忽略环境里的 http_proxy/https_proxy —— 被调用方（Jenkins、
  JumpServer）都在内网，走代理只会拿到 502（manager 侧踩过同一个坑）；
- 默认不跟随重定向：Jenkins 触发构建要读 201 的 Location 头，不能被吃掉；
- HTTP 错误码当正常响应返回（403 要取 crumb 重试、401 要给专门文案），
  只有连接层失败才抛 HttpError。

阻塞调用一律由调用方经 asyncio.to_thread 出让事件循环，避免卡住心跳。
"""
import json as jsonlib
import ssl
import urllib.error
import urllib.parse
import urllib.request

DEFAULT_TIMEOUT = 30  # 单次请求超时（秒）


class HttpError(Exception):
    """连接层失败（DNS/连接拒绝/超时/TLS 等）；HTTP 错误码不走这里。"""


class Response:
    """极简响应对象。"""

    __slots__ = ('status', 'headers', 'text')

    def __init__(self, status, headers, text):
        self.status = status
        self.headers = headers
        self.text = text

    @property
    def ok(self):
        return 200 <= self.status < 300

    def json(self):
        """响应体解析为 dict；非 JSON 或非对象返回 {}。列表体用 json_any。"""
        data = self.json_any()
        return data if isinstance(data, dict) else {}

    def json_any(self):
        """响应体解析为原始 JSON 值（dict / list / None）。"""
        try:
            return jsonlib.loads(self.text)
        except (ValueError, TypeError):
            return None


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """禁用自动重定向：201 + Location 是有效返回，不该被跟掉。"""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def build_ssl_context(verify_tls=True, ca_file=''):
    """TLS 上下文：verify_tls=False 时不校验（自签且不便配 CA 的内网服务）。"""
    if not verify_tls:
        return ssl._create_unverified_context()
    return ssl.create_default_context(cafile=ca_file or None)


class HttpClient:
    """同步 HTTP 客户端（供 asyncio.to_thread 调用）。"""

    def __init__(self, headers=None, verify_tls=True, ca_file='',
                 timeout=DEFAULT_TIMEOUT, follow_redirects=False):
        self._headers = dict(headers or {})
        self._timeout = timeout
        handlers = [
            urllib.request.ProxyHandler({}),
            urllib.request.HTTPSHandler(context=build_ssl_context(verify_tls, ca_file)),
        ]
        if not follow_redirects:
            handlers.insert(1, _NoRedirect())
        self._opener = urllib.request.build_opener(*handlers)

    def set_header(self, key, value):
        self._headers[key] = value

    def request(self, method, url, params=None, json_body=None) -> Response:
        """发一次请求。

        params 拼进 query；json_body 非空时以 JSON 提交。POST 无体时发空串
        （部分服务对 Content-Length 缺失的 POST 返回 411）。
        """
        if params:
            url = f'{url}?{urllib.parse.urlencode(params)}'
        headers = dict(self._headers)
        if json_body is not None:
            data = jsonlib.dumps(json_body, ensure_ascii=False).encode('utf-8')
            headers['Content-Type'] = 'application/json'
        elif method in ('POST', 'PUT', 'PATCH'):
            data = b''
        else:
            data = None
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with self._opener.open(req, timeout=self._timeout) as resp:
                body = resp.read().decode('utf-8', 'replace')
                return Response(resp.status, dict(resp.headers), body)
        except urllib.error.HTTPError as exc:
            body = exc.read().decode('utf-8', 'replace')
            return Response(exc.code, dict(exc.headers or {}), body)
        except urllib.error.URLError as exc:
            raise HttpError(f'请求失败（{url}）：{exc.reason}') from exc
        except (OSError, ValueError) as exc:
            raise HttpError(f'请求失败（{url}）：{exc}') from exc
