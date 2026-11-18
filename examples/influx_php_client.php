<?php
declare(strict_types=1);

/**
 * 零依赖 InfluxDB 读写客户端（PHP 8.0+，需 curl / json 扩展）
 * 适配本项目 [influx] version = v1 | v2 | v3：
 *   v3 -> /api/v3/write_lp + /api/v3/query_sql   (SQL,  Bearer token)
 *   v2 -> /api/v2/write    + /api/v2/query       (Flux, Token token)
 *   v1 -> /write           + /query              (InfluxQL, Basic auth)
 *
 * 运行：php examples/influx_php_client.php
 */

final class InfluxClient
{
    public function __construct(
        private readonly string $url,              // 例：http://127.0.0.1:8181
        private readonly string $version,          // v1 | v2 | v3
        private readonly string $database,         // v3/v1=db，v2=bucket
        private readonly string $org = '',         // v2 必填
        private readonly string $token = '',       // v2 Token / v3 Bearer
        private readonly string $username = '',    // v1 Basic auth
        private readonly string $password = '',
        private readonly float  $timeout = 10.0,
    ) {
        if (!in_array($version, ['v1', 'v2', 'v3'], true)) {
            throw new InvalidArgumentException("version 只能是 v1/v2/v3: {$version}");
        }
    }

    /**
     * 批量写入。$points 每项：
     * [
     *   'measurement' => 'mqtt_data_202611',
     *   'tags'        => ['devicecode' => 'TT10002'],  // tag 值会转字符串
     *   'fields'      => ['code' => 1.0, 'temp' => 22.5],
     *   'timestamp'   => 1793936103,                   // Unix 秒，内部转纳秒
     * ]
     */
    public function write(array $points): void
    {
        if (!$points) {
            return;
        }
        $lines = [];
        foreach ($points as $p) {
            $lines[] = self::lineProtocol($p);
        }
        $body = implode("\n", $lines) . "\n";

        [$path, $headers] = match ($this->version) {
            'v3' => [
                '/api/v3/write_lp?' . http_build_query([
                    'db' => $this->database, 'precision' => 'nanosecond',
                ]),
                $this->authHeaders(['Content-Type' => 'text/plain; charset=utf-8']),
            ],
            'v2' => [
                '/api/v2/write?' . http_build_query([
                    'org' => $this->org, 'bucket' => $this->database, 'precision' => 'ns',
                ]),
                $this->authHeaders(['Content-Type' => 'text/plain; charset=utf-8']),
            ],
            'v1' => [
                '/write?' . http_build_query(['db' => $this->database, 'precision' => 'ns']),
                $this->authHeaders(['Content-Type' => 'text/plain; charset=utf-8']),
            ],
        };

        // 成功写入返回 204；4xx 说明 line protocol / schema / 权限有问题
        $this->request('POST', $path, $body, $headers, [200, 204]);
    }

    /**
     * 查询，返回响应原文。
     * v3 用 SQL，v2 用 Flux，v1 用 InfluxQL —— 不自动翻译方言。
     */
    public function query(string $statement): string
    {
        return match ($this->version) {
            'v3' => $this->request('POST', '/api/v3/query_sql',
                json_encode([
                    'db' => $this->database, 'q' => $statement, 'format' => 'json',
                ], JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                $this->authHeaders([
                    'Content-Type' => 'application/json', 'Accept' => 'application/json',
                ]),
            )[1],
            'v2' => $this->request('POST', '/api/v2/query?' . http_build_query(['org' => $this->org]),
                json_encode([
                    'query' => $statement,
                    'dialect' => ['header' => true, 'dateTimeFormat' => 'RFC3339Nano'],
                ], JSON_UNESCAPED_UNICODE | JSON_THROW_ON_ERROR),
                $this->authHeaders([
                    'Content-Type' => 'application/json', 'Accept' => 'text/csv',
                ]),
            )[1],
            'v1' => $this->request('POST', '/query?' . http_build_query(['db' => $this->database]),
                http_build_query(['q' => $statement]),
                $this->authHeaders(['Content-Type' => 'application/x-www-form-urlencoded']),
            )[1],
        };
    }

    /**
     * 构造单行 Line Protocol。公开出来便于单独调试/打日志。
     * 转义：measurement / tag key / tag value / field key 中的空格、逗号、等号；
     * string field 中的反斜杠和双引号。
     */
    public static function lineProtocol(array $point): string
    {
        $measurement = self::esc((string) $point['measurement'], true);
        foreach ($point['tags'] ?? [] as $k => $v) {
            $measurement .= ',' . self::esc((string) $k, true) . '=' . self::esc((string) $v, true);
        }

        $fields = [];
        foreach ($point['fields'] ?? [] as $k => $v) {
            if ($v === null) {
                continue; // InfluxDB 不接受 null field
            }
            $key = self::esc((string) $k, true);
            if (is_int($v) || is_float($v)) {
                $fields[] = $key . '=' . (is_float($v) ? self::num($v) : $v);
            } elseif (is_bool($v)) {
                $fields[] = $key . '=' . ($v ? 'true' : 'false');
            } else {
                $s = str_replace(['\\', '"'], ['\\\\', '\\"'], (string) $v);
                $fields[] = $key . '="' . $s . '"';
            }
        }
        if (!$fields) {
            throw new InvalidArgumentException('至少需要一个非 null field');
        }

        // 时间戳统一纳秒；传毫秒需自行改成 (int)($ts * 1_000_000) 并同步改 precision
        $ts = (int) ($point['timestamp'] ?? time()) * 1_000_000_000;
        return $measurement . ' ' . implode(',', $fields) . ' ' . $ts;
    }

    private static function esc(string $s, bool $measurementContext): string
    {
        $s = str_replace(' ', '\\ ', $s);
        $s = str_replace(',', '\\,', $s);
        return $measurementContext ? str_replace('=', '\\=', $s) : $s;
    }

    private static function num(float $v): string
    {
        if (is_nan($v) || is_infinite($v)) {
            throw new InvalidArgumentException('field 不能是 NaN/Inf');
        }
        // 避免科学计数法（1.0E+6）被误读
        return number_format($v, 10, '.', '');
    }

    private function authHeaders(array $headers): array
    {
        $auth = match ($this->version) {
            'v3' => $this->token !== '' ? 'Bearer ' . $this->token : null,
            'v2' => $this->token !== '' ? 'Token ' . $this->token : null,
            'v1' => $this->username !== ''
                ? 'Basic ' . base64_encode($this->username . ':' . $this->password)
                : null,
        };
        if ($auth !== null) {
            $headers['Authorization'] = $auth;
        }
        return $headers;
    }

    /** @return array{0:int, 1:string} [HTTP 状态码, 响应体] */
    private function request(
        string $method, string $path, ?string $body, array $headers, array $okCodes = [200]
    ): array {
        $ch = curl_init(rtrim($this->url, '/') . $path);
        $flat = [];
        foreach ($headers as $k => $v) {
            $flat[] = $k . ': ' . $v;
        }
        curl_setopt_array($ch, [
            CURLOPT_CUSTOMREQUEST  => $method,
            CURLOPT_POSTFIELDS     => $body,
            CURLOPT_HTTPHEADER     => $flat,
            CURLOPT_RETURNTRANSFER => true,
            CURLOPT_CONNECTTIMEOUT => (int) $this->timeout,
            CURLOPT_TIMEOUT        => $this->timeout,
        ]);
        $raw = curl_exec($ch);
        if ($raw === false) {
            $err = curl_error($ch);
            curl_close($ch);
            throw new RuntimeException('InfluxDB 请求失败: ' . $err);
        }
        $status = (int) curl_getinfo($ch, CURLINFO_RESPONSE_CODE);
        curl_close($ch);

        if (!in_array($status, $okCodes, true)) {
            throw new RuntimeException("InfluxDB HTTP {$status}: " . substr($raw, 0, 500));
        }
        return [$status, (string) $raw];
    }
}

// ---------------- 使用示例（按本项目 config.tt1000.toml 的 v3 配置） ----------------
// 直接运行本文件时才执行；被 include 时只提供 InfluxClient 类。
if (PHP_SAPI === 'cli' && realpath($_SERVER['SCRIPT_FILENAME'] ?? '') === __FILE__) {
    $ver = getenv('INFLUX_VERSION') ?: 'v3';
    $client = new InfluxClient(
        url:      getenv('INFLUX_URL') ?: 'http://127.0.0.1:8181',
        version:  $ver,
        database: getenv('INFLUX_DB') ?: 'mqtt',
        org:      getenv('INFLUX_ORG') ?: '',       // v2 必填，如 'my-org'
        token:    getenv('INFLUX_TOKEN') ?: '',     // v2/v3；v3 留空 = 未开启鉴权
        username: getenv('INFLUX_USER') ?: '',      // v1
        password: getenv('INFLUX_PASS') ?: '',      // v1
    );

    // 1) 写：单条 / 批量同一方法。timestamp 为 Unix 秒，内部转纳秒。
    $client->write([
        [
            'measurement' => 'mqtt_data_202611',
            'tags'        => ['devicecode' => 'TT10002', 'event' => 'alarms'],
            'fields'      => ['code' => 1.0, 'temp' => 22.5, 'note' => 'ok,"x"'],
            'timestamp'   => 1793936103,
        ],
        [
            'measurement' => 'mqtt_data_202611',
            'tags'        => ['devicecode' => 'TT10003'],
            'fields'      => ['code' => 0.0],
            'timestamp'   => 1793936104,
        ],
    ]);
    echo "写入成功 (HTTP 204)\n";

    // 2) 读：方言随版本切换（不自动翻译）。
    $query = match ($ver) {
        'v3' => "SELECT time, devicecode, code FROM mqtt_data_202611 ORDER BY time DESC LIMIT 10",
        'v2' => "from(bucket: \"mqtt\") |> range(start: -1h) |> filter(fn: (r) => r._measurement == \"mqtt_data_202611\")",
        'v1' => "SELECT * FROM mqtt_data_202611 ORDER BY time DESC LIMIT 10",
    };
    echo $client->query($query), "\n";
}
