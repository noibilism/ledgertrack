<!-- Generator: Widdershins v4.0.1 -->

<h1 id="ledger-api">Ledger API v2</h1>

> Scroll down for code samples, example requests and responses. Select a language for code samples from the tabs above or the mobile navigation menu.

Base URLs:

* <a href="http://localhost:8080/">http://localhost:8080/</a>

# Authentication

- oAuth2 authentication. 

    - Flow: clientCredentials

    - Token URL = [/oauth/token](/oauth/token)

|Scope|Scope Description|
|---|---|

<h1 id="ledger-api-ledger">ledger</h1>

## Show server information

<a id="opIdv2GetInfo"></a>

> Code samples

```http
GET http://localhost:8080/_/info HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /_/info`

> Example responses

> 200 Response

```json
{
  "server": "string",
  "version": "string"
}
```

<h3 id="show-server-information-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ConfigInfo](#schemav2configinfo)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|
|5XX|Unknown|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Read in memory metrics

<a id="opIdgetMetrics"></a>

> Code samples

```http
GET http://localhost:8080/_/metrics HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /_/metrics`

> Example responses

> 200 Response

```json
{
  "property1": null,
  "property2": null
}
```

<h3 id="read-in-memory-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="read-in-memory-metrics-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» **additionalProperties**|any|false|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

<h1 id="ledger-api-ledger-v2">ledger.v2</h1>

## List ledgers

<a id="opIdv2ListLedgers"></a>

> Code samples

```http
GET http://localhost:8080/v2 HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2`

> Body parameter

```json
{}
```

<h3 id="list-ledgers-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|includeDeleted|query|boolean|false|If true, include deleted ledgers in the results. By default, deleted ledgers are excluded.|
|sort|query|string|false|Sort results using a field name and order (ascending or descending). |
|body|body|object|true|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

**includeDeleted**: If true, include deleted ledgers in the results. By default, deleted ledgers are excluded.

**sort**: Sort results using a field name and order (ascending or descending). 
Format: `<field>:<order>`, where `<field>` is the field name and `<order>` is either `asc` or `desc`.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "name": "string",
        "addedAt": "2019-08-24T14:15:22Z",
        "bucket": "string",
        "deletedAt": "2019-08-24T14:15:22Z",
        "metadata": {
          "admin": "true"
        },
        "features": {
          "property1": "string",
          "property2": "string"
        },
        "id": 0
      }
    ]
  }
}
```

<h3 id="list-ledgers-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2LedgerListResponse](#schemav2ledgerlistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get a ledger

<a id="opIdv2GetLedger"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}`

<h3 id="get-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{
  "data": {
    "name": "string",
    "addedAt": "2019-08-24T14:15:22Z",
    "bucket": "string",
    "deletedAt": "2019-08-24T14:15:22Z",
    "metadata": {
      "admin": "true"
    },
    "features": {
      "property1": "string",
      "property2": "string"
    },
    "id": 0
  }
}
```

<h3 id="get-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GetLedgerResponse](#schemav2getledgerresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Create a ledger

<a id="opIdv2CreateLedger"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger} HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}`

> Body parameter

```json
{
  "bucket": "string",
  "metadata": {
    "admin": "true"
  },
  "features": {
    "property1": "string",
    "property2": "string"
  }
}
```

<h3 id="create-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[V2CreateLedgerRequest](#schemav2createledgerrequest)|true|none|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="create-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Insert a schema for a ledger

<a id="opIdv2InsertSchema"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/schema/{version} HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/schema/{version}`

> Body parameter

```json
{
  "chart": {
    "users": {
      "$userID": {
        ".pattern": "^[0-9]{16}$"
      }
    }
  },
  "transactions": {
    "property1": {
      "description": "string",
      "script": "string",
      "runtime": "experimental-interpreter"
    },
    "property2": {
      "description": "string",
      "script": "string",
      "runtime": "experimental-interpreter"
    }
  }
}
```

<h3 id="insert-a-schema-for-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|Idempotency-Key|header|string|false|Use an idempotency key|
|body|body|[V2SchemaData](#schemav2schemadata)|true|none|
|ledger|path|string|true|Name of the ledger.|
|version|path|string|true|Schema version.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="insert-a-schema-for-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Schema inserted successfully|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Idempotency-Hit|string||Indicates that the request was processed using an idempotency key that was already used|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get a schema for a ledger by version

<a id="opIdv2GetSchema"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/schema/{version} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/schema/{version}`

<h3 id="get-a-schema-for-a-ledger-by-version-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|version|path|string|true|Schema version.|

> Example responses

> 200 Response

```json
{
  "data": {
    "version": "v1.0.0",
    "createdAt": "2023-01-01T00:00:00Z",
    "chart": {
      "users": {
        "$userID": {
          ".pattern": "^[0-9]{16}$"
        }
      }
    },
    "transactions": {
      "property1": {
        "description": "string",
        "script": "string",
        "runtime": "experimental-interpreter"
      },
      "property2": {
        "description": "string",
        "script": "string",
        "runtime": "experimental-interpreter"
      }
    }
  }
}
```

<h3 id="get-a-schema-for-a-ledger-by-version-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Schema retrieved successfully|[V2SchemaResponse](#schemav2schemaresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## List all schemas for a ledger

<a id="opIdv2ListSchemas"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/schema HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/schema`

<h3 id="list-all-schemas-for-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|cursor|query|string|false|The pagination cursor value|
|pageSize|query|integer|false|The maximum number of results to return per page|
|sort|query|string|false|The field to sort by|
|order|query|string|false|The sort order|
|ledger|path|string|true|Name of the ledger.|

#### Enumerated Values

|Parameter|Value|
|---|---|
|sort|created_at|
|order|asc|
|order|desc|

> Example responses

> 200 Response

```json
{
  "cursor": {
    "data": [
      {
        "version": "v1.0.0",
        "createdAt": "2023-01-01T00:00:00Z",
        "chart": {
          "users": {
            "$userID": {
              ".pattern": "^[0-9]{16}$"
            }
          }
        },
        "transactions": {
          "property1": {
            "description": "string",
            "script": "string",
            "runtime": "experimental-interpreter"
          },
          "property2": {
            "description": "string",
            "script": "string",
            "runtime": "experimental-interpreter"
          }
        }
      }
    ],
    "hasMore": true,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "pageSize": 0
  }
}
```

<h3 id="list-all-schemas-for-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Schemas retrieved successfully|[V2SchemasCursorResponse](#schemav2schemascursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Update ledger metadata

<a id="opIdv2UpdateLedgerMetadata"></a>

> Code samples

```http
PUT http://localhost:8080/v2/{ledger}/metadata HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`PUT /v2/{ledger}/metadata`

> Body parameter

```json
{
  "admin": "true"
}
```

<h3 id="update-ledger-metadata-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[V2Metadata](#schemav2metadata)|true|none|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="update-ledger-metadata-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|
|5XX|Unknown|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Delete ledger metadata by key

<a id="opIdv2DeleteLedgerMetadata"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/{ledger}/metadata/{key} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`DELETE /v2/{ledger}/metadata/{key}`

<h3 id="delete-ledger-metadata-by-key-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|key|path|string|true|Key to remove.|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="delete-ledger-metadata-by-key-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get information about a ledger

<a id="opIdv2GetLedgerInfo"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/_info HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/_info`

<h3 id="get-information-about-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{
  "data": {
    "name": "ledger001",
    "storage": {
      "migrations": [
        {
          "version": 11,
          "name": "migrations:001",
          "date": "2019-08-24T14:15:22Z",
          "state": "TO DO"
        }
      ]
    }
  }
}
```

<h3 id="get-information-about-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2LedgerInfoResponse](#schemav2ledgerinforesponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Bulk request

<a id="opIdv2CreateBulk"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/_bulk HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/_bulk`

> Body parameter

```json
[
  {
    "action": "string",
    "ik": "string",
    "data": {
      "timestamp": "2019-08-24T14:15:22Z",
      "postings": [
        {
          "amount": 100,
          "asset": "COIN",
          "destination": "users:002",
          "source": "users:001"
        }
      ],
      "script": {
        "template": "CUSTOMER_DEPOSIT",
        "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
        "vars": {
          "user": "users:042"
        }
      },
      "runtime": "experimental-interpreter",
      "reference": "ref:001",
      "metadata": {
        "admin": "true"
      },
      "accountMetadata": {
        "property1": {
          "admin": "true"
        },
        "property2": {
          "admin": "true"
        }
      },
      "force": true
    }
  }
]
```

<h3 id="bulk-request-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|continueOnFailure|query|boolean|false|Continue on failure|
|atomic|query|boolean|false|Make bulk atomic|
|parallel|query|boolean|false|Process bulk elements in parallel|
|schemaVersion|query|string|false|Default schema version to use for validation (can be overridden per element)|
|body|body|[V2Bulk](#schemav2bulk)|true|none|

> Example responses

> 200 Response

```json
{
  "data": [
    {
      "responseType": "string",
      "logID": 0,
      "data": {
        "insertedAt": "2019-08-24T14:15:22Z",
        "updatedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "template": "string"
      }
    }
  ],
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param"
}
```

<h3 id="bulk-request-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2BulkResponse](#schemav2bulkresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|OK|[V2BulkResponse](#schemav2bulkresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Count the accounts from a ledger

<a id="opIdv2CountAccounts"></a>

> Code samples

```http
HEAD http://localhost:8080/v2/{ledger}/accounts HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`HEAD /v2/{ledger}/accounts`

> Body parameter

```json
{}
```

<h3 id="count-the-accounts-from-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pit|query|string(date-time)|false|none|
|body|body|object|true|none|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="count-the-accounts-from-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Count|integer|bigint|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## List accounts from a ledger

<a id="opIdv2ListAccounts"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/accounts HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/accounts`

List accounts from a ledger, sorted by address in descending order.

> Body parameter

```json
{}
```

<h3 id="list-accounts-from-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|
|sort|query|string|false|Sort results using a field name and order (ascending or descending). |
|body|body|object|true|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

**sort**: Sort results using a field name and order (ascending or descending). 
Format: `<field>:<order>`, where `<field>` is the field name and `<order>` is either `asc` or `desc`.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "address": "users:001",
        "metadata": {
          "admin": "true"
        },
        "insertionDate": "2023-01-01T00:00:00Z",
        "updatedAt": "2023-01-01T00:00:00Z",
        "firstUsage": "2023-01-01T00:00:00Z",
        "volumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        },
        "effectiveVolumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        }
      }
    ]
  }
}
```

<h3 id="list-accounts-from-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2AccountsCursorResponse](#schemav2accountscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get account by its address

<a id="opIdv2GetAccount"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/accounts/{address} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/accounts/{address}`

<h3 id="get-account-by-its-address-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|address|path|string|true|Exact address of the account. It must match the following regular expressions pattern:|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|

#### Detailed descriptions

**address**: Exact address of the account. It must match the following regular expressions pattern:
```
^\w+(:\w+)*$
```

> Example responses

> 200 Response

```json
{
  "data": {
    "address": "users:001",
    "metadata": {
      "admin": "true"
    },
    "insertionDate": "2023-01-01T00:00:00Z",
    "updatedAt": "2023-01-01T00:00:00Z",
    "firstUsage": "2023-01-01T00:00:00Z",
    "volumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "effectiveVolumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  }
}
```

<h3 id="get-account-by-its-address-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2AccountResponse](#schemav2accountresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Add metadata to an account

<a id="opIdv2AddMetadataToAccount"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/accounts/{address}/metadata HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/accounts/{address}/metadata`

> Body parameter

```json
{
  "admin": "true"
}
```

<h3 id="add-metadata-to-an-account-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|address|path|string|true|Exact address of the account. It must match the following regular expressions pattern:|
|dryRun|query|boolean|false|Set the dry run mode. Dry run mode doesn't add the logs to the database or publish a message to the message broker.|
|Idempotency-Key|header|string|false|Use an idempotency key|
|schemaVersion|query|string|false|Schema version to use for validation|
|body|body|[V2Metadata](#schemav2metadata)|true|metadata|

#### Detailed descriptions

**address**: Exact address of the account. It must match the following regular expressions pattern:
```
^\w+(:\w+)*$
```

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="add-metadata-to-an-account-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|No Content|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="add-metadata-to-an-account-responseschema">Response Schema</h3>

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Idempotency-Hit|string||Indicates that the request was processed using an idempotency key that was already used|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Delete metadata by key

<a id="opIdv2DeleteTransactionMetadata"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/{ledger}/transactions/{id}/metadata/{key} HTTP/1.1
Host: localhost:8080
Accept: application/json
Idempotency-Key: string

```

`DELETE /v2/{ledger}/transactions/{id}/metadata/{key}`

Delete metadata by key

<h3 id="delete-metadata-by-key-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|key|path|string|true|The key to remove.|
|Idempotency-Key|header|string|false|Use an idempotency key|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="delete-metadata-by-key-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Key deleted|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="delete-metadata-by-key-responseschema">Response Schema</h3>

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Idempotency-Hit|string||Indicates that the request was processed using an idempotency key that was already used|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get statistics from a ledger

<a id="opIdv2ReadStats"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/stats HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/stats`

Get statistics from a ledger. (aggregate metrics on accounts and transactions)

<h3 id="get-statistics-from-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|name of the ledger|

> Example responses

> 200 Response

```json
{
  "data": {
    "accounts": 0,
    "transactions": 0
  }
}
```

<h3 id="get-statistics-from-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2StatsResponse](#schemav2statsresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Count the transactions from a ledger

<a id="opIdv2CountTransactions"></a>

> Code samples

```http
HEAD http://localhost:8080/v2/{ledger}/transactions HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`HEAD /v2/{ledger}/transactions`

> Body parameter

```json
{}
```

<h3 id="count-the-transactions-from-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pit|query|string(date-time)|false|none|
|body|body|object|true|none|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="count-the-transactions-from-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Count|integer|int64|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## List transactions from a ledger

<a id="opIdv2ListTransactions"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/transactions HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/transactions`

List transactions from a ledger, sorted by id in descending order.

> Body parameter

```json
{}
```

<h3 id="list-transactions-from-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|
|order|query|string|false|Deprecated: Use sort param|
|reverse|query|boolean|false|none|
|sort|query|string|false|Sort results using a field name and order (ascending or descending). |
|body|body|object|true|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

**sort**: Sort results using a field name and order (ascending or descending). 
Format: `<field>:<order>`, where `<field>` is the field name and `<order>` is either `asc` or `desc`.

#### Enumerated Values

|Parameter|Value|
|---|---|
|order|effective|

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "insertedAt": "2019-08-24T14:15:22Z",
        "updatedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "template": "string"
      }
    ]
  }
}
```

<h3 id="list-transactions-from-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2TransactionsCursorResponse](#schemav2transactionscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Create a new transaction to a ledger

<a id="opIdv2CreateTransaction"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/transactions HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/transactions`

> Body parameter

```json
{
  "timestamp": "2019-08-24T14:15:22Z",
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "script": {
    "template": "CUSTOMER_DEPOSIT",
    "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
    "vars": {
      "user": "users:042"
    }
  },
  "runtime": "experimental-interpreter",
  "reference": "ref:001",
  "metadata": {
    "admin": "true"
  },
  "accountMetadata": {
    "property1": {
      "admin": "true"
    },
    "property2": {
      "admin": "true"
    }
  },
  "force": true
}
```

<h3 id="create-a-new-transaction-to-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|dryRun|query|boolean|false|Set the dryRun mode. dry run mode doesn't add the logs to the database or publish a message to the message broker.|
|Idempotency-Key|header|string|false|Use an idempotency key|
|force|query|boolean|false|Disable balance checks when passing postings|
|schemaVersion|query|string|false|Schema version to use for validation|
|body|body|[V2PostTransaction](#schemav2posttransaction)|true|The request body must contain at least one of the following objects:|

#### Detailed descriptions

**body**: The request body must contain at least one of the following objects:
  - `postings`: suitable for simple transactions
  - `script`: enabling more complex transactions with Numscript

> Example responses

> 200 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}
```

<h3 id="create-a-new-transaction-to-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|200|Idempotency-Hit|string||Indicates that the request was processed using an idempotency key that was already used|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get transaction from a ledger by its ID

<a id="opIdv2GetTransaction"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/transactions/{id} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/transactions/{id}`

<h3 id="get-transaction-from-a-ledger-by-its-id-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|expand|query|string|false|none|
|pit|query|string(date-time)|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}
```

<h3 id="get-transaction-from-a-ledger-by-its-id-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GetTransactionResponse](#schemav2gettransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Set the metadata of a transaction by its ID

<a id="opIdv2AddMetadataOnTransaction"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/transactions/{id}/metadata HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/transactions/{id}/metadata`

> Body parameter

```json
{
  "admin": "true"
}
```

<h3 id="set-the-metadata-of-a-transaction-by-its-id-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|dryRun|query|boolean|false|Set the dryRun mode. Dry run mode doesn't add the logs to the database or publish a message to the message broker.|
|Idempotency-Key|header|string|false|Use an idempotency key|
|schemaVersion|query|string|false|Schema version to use for validation|
|body|body|[V2Metadata](#schemav2metadata)|true|metadata|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="set-the-metadata-of-a-transaction-by-its-id-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|No Content|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="set-the-metadata-of-a-transaction-by-its-id-responseschema">Response Schema</h3>

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|204|Idempotency-Hit|string||Indicates that the request was processed using an idempotency key that was already used|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Revert a ledger transaction by its ID

<a id="opIdv2RevertTransaction"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/transactions/{id}/revert HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/transactions/{id}/revert`

> Body parameter

```json
{
  "metadata": {
    "property1": "string",
    "property2": "string"
  }
}
```

<h3 id="revert-a-ledger-transaction-by-its-id-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|id|path|integer(bigint)|true|Transaction ID.|
|force|query|boolean|false|Force revert|
|atEffectiveDate|query|boolean|false|Revert transaction at effective date of the original tx|
|dryRun|query|boolean|false|Set the dryRun mode. dry run mode doesn't add the logs to the database or publish a message to the message broker.|
|schemaVersion|query|string|false|Schema version to use for validation|
|Idempotency-Key|header|string|false|Use an idempotency key|
|body|body|[V2RevertTransactionRequest](#schemav2reverttransactionrequest)|false|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}
```

<h3 id="revert-a-ledger-transaction-by-its-id-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|OK|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

### Response Headers

|Status|Header|Type|Format|Description|
|---|---|---|---|---|
|201|Idempotency-Hit|string||Indicates that the request was processed using an idempotency key that was already used|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get the aggregated balances from selected accounts

<a id="opIdv2GetBalancesAggregated"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/aggregate/balances HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/aggregate/balances`

> Body parameter

```json
{}
```

<h3 id="get-the-aggregated-balances-from-selected-accounts-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pit|query|string(date-time)|false|none|
|useInsertionDate|query|boolean|false|Use insertion date instead of effective date|
|body|body|object|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "USD": 100,
    "EUR": 12
  }
}
```

<h3 id="get-the-aggregated-balances-from-selected-accounts-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2AggregateBalancesResponse](#schemav2aggregatebalancesresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get list of volumes with balances for (account/asset)

<a id="opIdv2GetVolumesWithBalances"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/volumes HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/volumes`

> Body parameter

```json
{}
```

<h3 id="get-list-of-volumes-with-balances-for-(account/asset)-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|ledger|path|string|true|Name of the ledger.|
|endTime|query|string(date-time)|false|none|
|startTime|query|string(date-time)|false|none|
|insertionDate|query|boolean|false|Use insertion date instead of effective date|
|groupBy|query|integer(int64)|false|Group volumes and balance by the level of the segment of the address|
|sort|query|string|false|Sort results using a field name and order (ascending or descending). |
|body|body|object|true|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

**sort**: Sort results using a field name and order (ascending or descending). 
Format: `<field>:<order>`, where `<field>` is the field name and `<order>` is either `asc` or `desc`.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "account": "string",
        "asset": "string",
        "input": 0,
        "output": 0,
        "balance": 0
      }
    ]
  }
}
```

<h3 id="get-list-of-volumes-with-balances-for-(account/asset)-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2VolumesWithBalanceCursorResponse](#schemav2volumeswithbalancecursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Create a new wallet

<a id="opIdv2CreateWallet"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/wallets HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/wallets`

> Body parameter

```json
false
```

<h3 id="create-a-new-wallet-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|Idempotency-Key|header|string|false|Use an idempotency key|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="create-a-new-wallet-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Wallet created|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="create-a-new-wallet-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List configured currencies

<a id="opIdv2ListCurrencies"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/currencies HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/currencies`

Returns the database-backed wallet currency registry loaded from `_system.currencies`.

<h3 id="list-configured-currencies-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{
  "data": {
    "currencies": [
      {
        "code": "USD",
        "precision": 2,
        "enabled": true
      }
    ]
  }
}
```

<h3 id="list-configured-currencies-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CurrenciesResponse](#schemav2currenciesresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Create a fee override

<a id="opIdv2CreateFeeOverride"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/fee-overrides HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/fee-overrides`

Replaces the product's transaction fees for one event, for one client (optionally one product) or one account. `rules` are all the fee rules for the event, so a combined fee (flat plus percentage) is one override. Resolution order is account override, then client and product override, then client-wide override in the same currency, then the product default. Overlapping overrides for the same target are rejected with 409. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "scope": "client",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "NGN",
  "event": "debit",
  "rules": [
    {
      "event": "debit",
      "type": "flat",
      "value": "string",
      "tiers": [
        {
          "from": "0",
          "to": "string",
          "type": "flat",
          "value": "string"
        }
      ],
      "min": "string",
      "max": "string",
      "currency": "string"
    }
  ],
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "metadata": {}
}
```

<h3 id="create-a-fee-override-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|body|body|[V2CreateFeeOverrideRequest](#schemav2createfeeoverriderequest)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "string",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "event": "string",
    "rules": [
      {
        "event": "debit",
        "type": "flat",
        "value": "string",
        "tiers": [
          {
            "from": "0",
            "to": "string",
            "type": "flat",
            "value": "string"
          }
        ],
        "min": "string",
        "max": "string",
        "currency": "string"
      }
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="create-a-fee-override-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Fee override created|[V2FeeOverrideResponse](#schemav2feeoverrideresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List fee overrides

<a id="opIdv2ListFeeOverrides"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/fee-overrides HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/fee-overrides`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="list-fee-overrides-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|client_id|query|string(uuid)|false|none|
|account_id|query|string(uuid)|false|none|
|event|query|string|false|none|
|status|query|string|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|status|active|
|status|revoked|

> Example responses

> 200 Response

```json
{
  "data": {
    "overrides": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "scope": "string",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "string",
        "event": "string",
        "rules": [
          {
            "event": "debit",
            "type": "flat",
            "value": "string",
            "tiers": [
              {
                "from": "0",
                "to": "string",
                "type": "flat",
                "value": "string"
              }
            ],
            "min": "string",
            "max": "string",
            "currency": "string"
          }
        ],
        "effective_from": "2019-08-24T14:15:22Z",
        "effective_to": "2019-08-24T14:15:22Z",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="list-fee-overrides-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FeeOverrideListResponse](#schemav2feeoverridelistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get a fee override

<a id="opIdv2GetFeeOverride"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/fee-overrides/{id} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/fee-overrides/{id}`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-a-fee-override-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "string",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "event": "string",
    "rules": [
      {
        "event": "debit",
        "type": "flat",
        "value": "string",
        "tiers": [
          {
            "from": "0",
            "to": "string",
            "type": "flat",
            "value": "string"
          }
        ],
        "min": "string",
        "max": "string",
        "currency": "string"
      }
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-a-fee-override-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FeeOverrideResponse](#schemav2feeoverrideresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Revoke a fee override

<a id="opIdv2RevokeFeeOverride"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/fee-overrides/{id}/revoke HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/fee-overrides/{id}/revoke`

Ends the override now. The product default (or the next override in the resolution order) applies from then on. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="revoke-a-fee-override-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "string",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "event": "string",
    "rules": [
      {
        "event": "debit",
        "type": "flat",
        "value": "string",
        "tiers": [
          {
            "from": "0",
            "to": "string",
            "type": "flat",
            "value": "string"
          }
        ],
        "min": "string",
        "max": "string",
        "currency": "string"
      }
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="revoke-a-fee-override-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FeeOverrideResponse](#schemav2feeoverrideresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Create a tax rule

<a id="opIdv2CreateTaxRule"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/tax-rules HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/tax-rules`

A tax rule applies to a client country (ISO 3166-1 alpha-2) and an event. `base` is `fee` (tax on the rounded fee) or `amount` (tax on the transaction amount). `payable_accounts` maps each currency to the liability GL account code that receives the tax. The country comes from the client's incorporation country, then the contact address country. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "country": "NG",
  "code": "VAT",
  "name": "Value added tax",
  "event": "debit",
  "base": "fee",
  "rate": "7.5",
  "min": "string",
  "max": "string",
  "payable_accounts": {
    "NGN": "202100.NGN"
  },
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "metadata": {}
}
```

<h3 id="create-a-tax-rule-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|body|body|[V2CreateTaxRuleRequest](#schemav2createtaxrulerequest)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "country": "string",
    "code": "string",
    "name": "string",
    "event": "string",
    "base": "string",
    "rate": "string",
    "min": "string",
    "max": "string",
    "payable_accounts": {
      "property1": "string",
      "property2": "string"
    },
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="create-a-tax-rule-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Tax rule created|[V2TaxRuleResponse](#schemav2taxruleresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List tax rules

<a id="opIdv2ListTaxRules"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/tax-rules HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/tax-rules`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="list-tax-rules-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|country|query|string|false|none|
|event|query|string|false|none|
|status|query|string|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|status|active|
|status|revoked|

> Example responses

> 200 Response

```json
{
  "data": {
    "rules": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "country": "string",
        "code": "string",
        "name": "string",
        "event": "string",
        "base": "string",
        "rate": "string",
        "min": "string",
        "max": "string",
        "payable_accounts": {
          "property1": "string",
          "property2": "string"
        },
        "effective_from": "2019-08-24T14:15:22Z",
        "effective_to": "2019-08-24T14:15:22Z",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="list-tax-rules-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2TaxRuleListResponse](#schemav2taxrulelistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get a tax rule

<a id="opIdv2GetTaxRule"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/tax-rules/{id} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/tax-rules/{id}`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-a-tax-rule-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "country": "string",
    "code": "string",
    "name": "string",
    "event": "string",
    "base": "string",
    "rate": "string",
    "min": "string",
    "max": "string",
    "payable_accounts": {
      "property1": "string",
      "property2": "string"
    },
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-a-tax-rule-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2TaxRuleResponse](#schemav2taxruleresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Revoke a tax rule

<a id="opIdv2RevokeTaxRule"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/tax-rules/{id}/revoke HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/tax-rules/{id}/revoke`

Ends the rule now. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="revoke-a-tax-rule-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "country": "string",
    "code": "string",
    "name": "string",
    "event": "string",
    "base": "string",
    "rate": "string",
    "min": "string",
    "max": "string",
    "payable_accounts": {
      "property1": "string",
      "property2": "string"
    },
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="revoke-a-tax-rule-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2TaxRuleResponse](#schemav2taxruleresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Create a business calendar

<a id="opIdv2CreateBusinessCalendar"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/business-calendars HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/business-calendars`

Defines business hours for reserve holds: an IANA `timezone`, ISO `working_days` (1 Monday to 7 Sunday), the opening window `open_time` to `close_time` (HH:MM, at least one hour) and `holidays` (YYYY-MM-DD) on which nothing counts. The code is unique. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "code": "string",
  "name": "string",
  "timezone": "Africa/Lagos",
  "working_days": [
    1
  ],
  "open_time": "08:00",
  "close_time": "17:00",
  "holidays": [
    "2019-08-24"
  ]
}
```

<h3 id="create-a-business-calendar-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|body|body|[V2BusinessCalendarRequest](#schemav2businesscalendarrequest)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "timezone": "string",
    "working_days": [
      0
    ],
    "open_time": "string",
    "close_time": "string",
    "holidays": [
      "2019-08-24"
    ],
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="create-a-business-calendar-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Created|[V2BusinessCalendarResponse](#schemav2businesscalendarresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List business calendars

<a id="opIdv2ListBusinessCalendars"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/business-calendars HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/business-calendars`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="list-business-calendars-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|

> Example responses

> 200 Response

```json
{
  "data": {
    "calendars": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "code": "string",
        "name": "string",
        "timezone": "string",
        "working_days": [
          0
        ],
        "open_time": "string",
        "close_time": "string",
        "holidays": [
          "2019-08-24"
        ],
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="list-business-calendars-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2BusinessCalendarListResponse](#schemav2businesscalendarlistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get a business calendar

<a id="opIdv2GetBusinessCalendar"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/business-calendars/{id} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/business-calendars/{id}`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-a-business-calendar-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "timezone": "string",
    "working_days": [
      0
    ],
    "open_time": "string",
    "close_time": "string",
    "holidays": [
      "2019-08-24"
    ],
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-a-business-calendar-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2BusinessCalendarResponse](#schemav2businesscalendarresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Replace a business calendar

<a id="opIdv2UpdateBusinessCalendar"></a>

> Code samples

```http
PUT http://localhost:8080/v2/{ledger}/business-calendars/{id} HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`PUT /v2/{ledger}/business-calendars/{id}`

Replaces the name, timezone, working days, hours and holidays. The code cannot change. Holds already scheduled keep their release time. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "code": "string",
  "name": "string",
  "timezone": "Africa/Lagos",
  "working_days": [
    1
  ],
  "open_time": "08:00",
  "close_time": "17:00",
  "holidays": [
    "2019-08-24"
  ]
}
```

<h3 id="replace-a-business-calendar-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|
|body|body|[V2BusinessCalendarRequest](#schemav2businesscalendarrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "timezone": "string",
    "working_days": [
      0
    ],
    "open_time": "string",
    "close_time": "string",
    "holidays": [
      "2019-08-24"
    ],
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="replace-a-business-calendar-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2BusinessCalendarResponse](#schemav2businesscalendarresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Create a reserve rule

<a id="opIdv2CreateReserveRule"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reserve-rules HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/reserve-rules`

Holds back a share of every credit into the customer's wallet. The held share stays in the ledger balance (the wallet lien) for `hold_hours`, the agreed SLA, and is then released to the available balance. A `client` rule covers the client's accounts in `currency`, optionally only those of `product_id`. An `account` rule covers one account and takes its currency from the account. Resolution order is account rule, then client and product rule, then client-wide rule. `percentage` is above 0 and at most 100 with up to 4 decimals. `hold_hours` is 1 to 8760. `hold_mode` is `calendar` (clock hours, default) or `business`, which counts only the hours inside the business calendar named by `calendar_id`. `credit_types` limits the rule to those credit types; a rule that names the credit type beats a general rule at the same level. Overlapping rules are rejected with 409: two general rules, or two rules sharing a credit type, for the same target and period. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "scope": "client",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "string",
  "percentage": "string",
  "hold_hours": 1,
  "hold_mode": "calendar",
  "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
  "credit_types": [
    "string"
  ],
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "metadata": {}
}
```

<h3 id="create-a-reserve-rule-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|body|body|[V2CreateReserveRuleRequest](#schemav2createreserverulerequest)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "client",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "percentage": "string",
    "hold_hours": 0,
    "hold_mode": "calendar",
    "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
    "credit_types": [
      "string"
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="create-a-reserve-rule-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Created|[V2ReserveRuleResponse](#schemav2reserveruleresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List reserve rules

<a id="opIdv2ListReserveRules"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reserve-rules HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reserve-rules`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="list-reserve-rules-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|client_id|query|string(uuid)|false|none|
|account_id|query|string(uuid)|false|none|
|status|query|string|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|status|active|
|status|revoked|

> Example responses

> 200 Response

```json
{
  "data": {
    "rules": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "scope": "client",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "string",
        "percentage": "string",
        "hold_hours": 0,
        "hold_mode": "calendar",
        "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
        "credit_types": [
          "string"
        ],
        "effective_from": "2019-08-24T14:15:22Z",
        "effective_to": "2019-08-24T14:15:22Z",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="list-reserve-rules-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ReserveRuleListResponse](#schemav2reserverulelistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get a reserve rule

<a id="opIdv2GetReserveRule"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reserve-rules/{id} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reserve-rules/{id}`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-a-reserve-rule-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "client",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "percentage": "string",
    "hold_hours": 0,
    "hold_mode": "calendar",
    "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
    "credit_types": [
      "string"
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-a-reserve-rule-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ReserveRuleResponse](#schemav2reserveruleresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Revoke a reserve rule

<a id="opIdv2RevokeReserveRule"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reserve-rules/{id}/revoke HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/reserve-rules/{id}/revoke`

Ends the rule now. Holds already made keep their release time. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="revoke-a-reserve-rule-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "client",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "percentage": "string",
    "hold_hours": 0,
    "hold_mode": "calendar",
    "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
    "credit_types": [
      "string"
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="revoke-a-reserve-rule-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ReserveRuleResponse](#schemav2reserveruleresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Preview the reserve on a credit

<a id="opIdv2PreviewReserve"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reserve-rules/preview HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/reserve-rules/preview`

Shows how a credit of `amount_minor` into an account would be split at `at` (default now): `reserve_minor` is held in the ledger balance until `release_at`, `available_minor` is available at once. The held share is the percentage of the amount rounded half-even to a whole minor unit. `credit_type` selects rules that list credit types. For business-hours rules, `release_at` counts only open hours on working days outside holidays. `rule_source` is `account`, `client` or `none`. Nothing is posted. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "amount_minor": 0,
  "credit_type": "string",
  "at": "2019-08-24T14:15:22Z"
}
```

<h3 id="preview-the-reserve-on-a-credit-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|body|body|[V2ReservePreviewRequest](#schemav2reservepreviewrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "currency": "string",
    "amount_minor": 0,
    "reserve_minor": 0,
    "available_minor": 0,
    "percentage": "string",
    "hold_hours": 0,
    "hold_mode": "calendar",
    "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
    "credit_type": "string",
    "release_at": "2019-08-24T14:15:22Z",
    "rule_source": "account",
    "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60"
  }
}
```

<h3 id="preview-the-reserve-on-a-credit-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ReservePreviewResponse](#schemav2reservepreviewresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Quote the fee and tax for a transaction

<a id="opIdv2QuoteFees"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/fees/quote HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/fees/quote`

Previews the fee and tax for an amount on an account, using the configuration in force at `at` (default now). Nothing is posted. Fee and tax lines are rounded half-even to the currency precision, tax on the fee uses the rounded fee, and `total` is amount plus fee plus tax. Returns 400 when tax rules exist for the event and the client's country is unknown. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "event": "debit",
  "amount_minor": 0,
  "at": "2019-08-24T14:15:22Z"
}
```

<h3 id="quote-the-fee-and-tax-for-a-transaction-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|body|body|[V2FeeQuoteRequest](#schemav2feequoterequest)|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "event": "string",
    "currency": "string",
    "amount": "string",
    "fee": "string",
    "tax": "string",
    "gross": "string",
    "total": "string",
    "amount_minor": 0,
    "fee_minor": 0,
    "tax_minor": 0,
    "total_minor": 0,
    "fee_source": "account",
    "fee_override_id": "77f4042c-f990-41c0-9a3f-ccbfd4e7b296",
    "fee_lines": [
      {
        "type": "string",
        "amount": "string"
      }
    ],
    "tax_lines": [
      {
        "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
        "code": "string",
        "base": "string",
        "rate": "string",
        "amount": "string",
        "payable_account_code": "string"
      }
    ]
  }
}
```

<h3 id="quote-the-fee-and-tax-for-a-transaction-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FeeQuoteResponse](#schemav2feequoteresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Create a GL account

<a id="opIdv2CreateGLAccount"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/gl/accounts HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/gl/accounts`

Adds an account to the GL registry (chart of accounts). The ledger address must be under the `gl:` prefix. Codes are unique, the parent must exist with the same currency and type. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "code": "1001",
  "name": "string",
  "type": "asset",
  "normal_side": "debit",
  "currency": "NGN",
  "parent_code": "string",
  "ledger": "ledgertrack",
  "address": "gl:assets:cash",
  "metadata": {}
}
```

<h3 id="create-a-gl-account-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|body|body|[V2CreateGLAccountRequest](#schemav2createglaccountrequest)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "type": "asset",
    "normal_side": "debit",
    "currency": "string",
    "parent_code": "string",
    "ledger": "string",
    "address": "string",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="create-a-gl-account-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|GL account created|[V2GLAccountResponse](#schemav2glaccountresponse)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|The code or the ledger address is already used|[V2ErrorResponse](#schemav2errorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List GL accounts

<a id="opIdv2ListGLAccounts"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/gl/accounts HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/gl/accounts`

Lists the GL registry ordered by code. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="list-gl-accounts-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|type|query|[V2GLAccountType](#schemav2glaccounttype)|false|none|
|currency|query|string|false|none|
|parent|query|string|false|Only return the direct children of this account code.|
|status|query|[V2GLAccountStatus](#schemav2glaccountstatus)|false|none|

#### Enumerated Values

|Parameter|Value|
|---|---|
|type|asset|
|type|liability|
|type|equity|
|type|income|
|type|expense|
|status|active|
|status|closed|

> Example responses

> 200 Response

```json
{
  "data": {
    "accounts": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "code": "string",
        "name": "string",
        "type": "asset",
        "normal_side": "debit",
        "currency": "string",
        "parent_code": "string",
        "ledger": "string",
        "address": "string",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="list-gl-accounts-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GLAccountListResponse](#schemav2glaccountlistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Import a chart of accounts

<a id="opIdv2ImportGLAccounts"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/gl/accounts/import HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/gl/accounts/import`

Bulk import of a chart, as a JSON array (`application/json`) or CSV (`text/csv`, header `code,name,type,normal_side,currency,parent_code,ledger,address`; `normal_side` and `parent_code` may be blank).
The import is all-or-nothing and idempotent: rows identical to an existing account are `unchanged`, rows that differ from an existing code are reported as `conflict` and are not applied. If any row is `invalid` or a `conflict`, nothing is written (400 or 409 with the report).
With `dry_run=true` the same validation runs and the per-row report is returned with status 200 without writing anything. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
[
  {
    "code": "1001",
    "name": "string",
    "type": "asset",
    "normal_side": "debit",
    "currency": "NGN",
    "parent_code": "string",
    "ledger": "ledgertrack",
    "address": "gl:assets:cash",
    "metadata": {}
  }
]
```

<h3 id="import-a-chart-of-accounts-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|dry_run|query|boolean|false|Validate and report without writing.|
|body|body|[V2CreateGLAccountRequest](#schemav2createglaccountrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "dry_run": true,
    "committed": true,
    "can_commit": true,
    "total": 0,
    "to_create": 0,
    "unchanged": 0,
    "conflicts": 0,
    "invalid": 0,
    "rows": [
      {
        "row": 0,
        "code": "string",
        "status": "create",
        "errors": [
          "string"
        ],
        "conflicts": [
          "string"
        ]
      }
    ]
  }
}
```

<h3 id="import-a-chart-of-accounts-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Dry-run report, or the import was applied (`committed` is true)|[V2GLImportReportResponse](#schemav2glimportreportresponse)|
|400|[Bad Request](https://tools.ietf.org/html/rfc7231#section-6.5.1)|Some rows are invalid, nothing was applied|[V2GLImportRejectedResponse](#schemav2glimportrejectedresponse)|
|409|[Conflict](https://tools.ietf.org/html/rfc7231#section-6.5.8)|Some rows conflict with existing accounts, nothing was applied|[V2GLImportRejectedResponse](#schemav2glimportrejectedresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get a GL account by code

<a id="opIdv2GetGLAccount"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/gl/accounts/{code} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/gl/accounts/{code}`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-a-gl-account-by-code-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|code|path|string|true|GL account code.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "type": "asset",
    "normal_side": "debit",
    "currency": "string",
    "parent_code": "string",
    "ledger": "string",
    "address": "string",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-a-gl-account-by-code-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GLAccountResponse](#schemav2glaccountresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Update a GL account

<a id="opIdv2PatchGLAccount"></a>

> Code samples

```http
PATCH http://localhost:8080/v2/{ledger}/gl/accounts/{code} HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`PATCH /v2/{ledger}/gl/accounts/{code}`

Only `name`, `metadata` (replaces the whole object) and `status` can change; any other field is rejected. An account with active children cannot be closed, and a child cannot be reopened under a closed parent. This endpoint is supported on the `ledgertrack` ledger.

> Body parameter

```json
{
  "name": "string",
  "status": "active",
  "metadata": {}
}
```

<h3 id="update-a-gl-account-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|code|path|string|true|GL account code.|
|body|body|[V2PatchGLAccountRequest](#schemav2patchglaccountrequest)|true|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "type": "asset",
    "normal_side": "debit",
    "currency": "string",
    "parent_code": "string",
    "ledger": "string",
    "address": "string",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="update-a-gl-account-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GLAccountResponse](#schemav2glaccountresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get the ledger balance of a GL account

<a id="opIdv2GetGLAccountBalance"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/gl/accounts/{code}/balance HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/gl/accounts/{code}/balance`

Reads the balance (input - output) at the account's ledger address, optionally at a point in time. A positive balance is a debit, a negative balance a credit. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-the-ledger-balance-of-a-gl-account-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|code|path|string|true|GL account code.|
|pit|query|string(date-time)|false|Point in time.|

> Example responses

> 200 Response

```json
{
  "data": {
    "code": "string",
    "name": "string",
    "type": "asset",
    "normal_side": "debit",
    "currency": "string",
    "parent_code": "string",
    "ledger": "string",
    "address": "string",
    "status": "active",
    "asset": "NGN/2",
    "pit": "2019-08-24T14:15:22Z",
    "balance": 0,
    "debit": 0,
    "credit": 0,
    "natural_balance": 0,
    "abnormal": true
  }
}
```

<h3 id="get-the-ledger-balance-of-a-gl-account-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GLAccountBalanceResponse](#schemav2glaccountbalanceresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## List accounts held by a client

<a id="opIdv2ListClientAccounts"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/clients/{clientID}/accounts HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/clients/{clientID}/accounts`

Returns the CBA accounts owned by a client. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="list-accounts-held-by-a-client-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|clientID|path|string(uuid)|true|CBA client ID.|

> Example responses

> 200 Response

```json
{
  "data": {
    "accounts": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "account_number": "0123456789",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "USD",
        "status": "active",
        "wallet_id": "client-CL-2026-000123-SAV-USD-001",
        "freeze_debits": false,
        "opened_at": "2019-08-24T14:15:22Z",
        "activated_at": "2019-08-24T14:15:22Z",
        "closed_at": "2019-08-24T14:15:22Z",
        "last_activity_at": "2019-08-24T14:15:22Z",
        "interest_accrued": "2.50",
        "metadata": {}
      }
    ]
  }
}
```

<h3 id="list-accounts-held-by-a-client-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CBAAccountListResponse](#schemav2cbaaccountlistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get client portfolio report

<a id="opIdv2GetClientPortfolioReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/clients/{clientID}/portfolio HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/clients/{clientID}/portfolio`

Returns the standard CBA portfolio view for a client. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-client-portfolio-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|clientID|path|string(uuid)|true|CBA client ID.|

> Example responses

> 200 Response

```json
{
  "data": {
    "client": {
      "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
      "client_number": "CL-2026-000123",
      "type": "individual",
      "status": "active",
      "kyc_level": 1,
      "kyc_status": "verified",
      "kyc_data": {},
      "contact": {
        "email": "string",
        "phone": "string",
        "address": {
          "line1": "string",
          "line2": "string",
          "city": "string",
          "state": "string",
          "country": "string",
          "postal": "string"
        }
      },
      "individual_data": {
        "first_name": "string",
        "middle_name": "string",
        "last_name": "string",
        "date_of_birth": "string",
        "gender": "string",
        "national_id_type": "string",
        "national_id_number": "string",
        "nationality": "string",
        "occupation": "string",
        "employer": "string"
      },
      "corporate_data": {
        "legal_name": "string",
        "trading_name": "string",
        "registration_number": "string",
        "tax_id": "string",
        "incorporation_date": "string",
        "incorporation_country": "string",
        "industry": "string",
        "beneficial_owners": [
          {
            "name": "string",
            "ownership_pct": "string",
            "national_id": "string"
          }
        ],
        "authorized_signatories": [
          {
            "client_id": "string",
            "role": "string",
            "signing_limit": "string"
          }
        ]
      },
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "accounts": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "account_number": "0123456789",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "USD",
        "status": "active",
        "wallet_id": "client-CL-2026-000123-SAV-USD-001",
        "freeze_debits": false,
        "opened_at": "2019-08-24T14:15:22Z",
        "activated_at": "2019-08-24T14:15:22Z",
        "closed_at": "2019-08-24T14:15:22Z",
        "last_activity_at": "2019-08-24T14:15:22Z",
        "interest_accrued": "2.50",
        "metadata": {},
        "available_balance": 10000
      }
    ],
    "total_accounts": 0,
    "totals_by_currency": [
      {
        "currency": "USD",
        "account_count": 0,
        "active_count": 0,
        "dormant_count": 0,
        "suspended_count": 0,
        "pending_count": 0,
        "closed_count": 0,
        "total_available_balance": 0,
        "total_interest_accrued": "2.50"
      }
    ]
  }
}
```

<h3 id="get-client-portfolio-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CBAClientPortfolioReportResponse](#schemav2cbaclientportfolioreportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get account statement report

<a id="opIdv2GetAccountStatementReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/accounts/{address}/statement HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/accounts/{address}/statement`

Returns the standard CBA account statement report for a single account. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-account-statement-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|address|path|string(uuid)|true|CBA account ID.|
|startTime|query|string(date-time)|false|none|
|endTime|query|string(date-time)|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "account": {
      "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
      "account_number": "0123456789",
      "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
      "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
      "currency": "USD",
      "status": "active",
      "wallet_id": "client-CL-2026-000123-SAV-USD-001",
      "freeze_debits": false,
      "opened_at": "2019-08-24T14:15:22Z",
      "activated_at": "2019-08-24T14:15:22Z",
      "closed_at": "2019-08-24T14:15:22Z",
      "last_activity_at": "2019-08-24T14:15:22Z",
      "interest_accrued": "2.50",
      "metadata": {}
    },
    "start_time": "2019-08-24T14:15:22Z",
    "end_time": "2019-08-24T14:15:22Z",
    "opening_balance": 0,
    "closing_balance": 0,
    "total_credits": 0,
    "total_debits": 0,
    "transaction_count": 0,
    "transactions": [
      {
        "txid": 0,
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "metadata": {
          "admin": "true"
        },
        "balance_before": 0,
        "balance_after": 0
      }
    ]
  }
}
```

<h3 id="get-account-statement-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CBAAccountStatementReportResponse](#schemav2cbaaccountstatementreportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get daily transaction summary report

<a id="opIdv2GetDailyTransactionSummaryReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/transactions/daily?date=2019-08-24 HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/transactions/daily`

Returns the standard CBA daily transaction summary for all accounts in scope. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-daily-transaction-summary-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|date|query|string(date)|true|Report date in `YYYY-MM-DD` format.|
|client_id|query|string(uuid)|false|Restrict the report to a client.|
|account_id|query|string(uuid)|false|Restrict the report to a single account.|

> Example responses

> 200 Response

```json
{
  "data": {
    "report_date": "2019-08-24",
    "transaction_count": 0,
    "debit_count": 0,
    "credit_count": 0,
    "debit_amount": 0,
    "credit_amount": 0,
    "net_amount": 0,
    "operation_counts": {
      "property1": 0,
      "property2": 0
    },
    "accounts": [
      {
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "account_number": "string",
        "currency": "string",
        "transaction_count": 0,
        "debit_count": 0,
        "credit_count": 0,
        "debit_amount": 0,
        "credit_amount": 0,
        "net_amount": 0,
        "operation_counts": {
          "property1": 0,
          "property2": 0
        }
      }
    ]
  }
}
```

<h3 id="get-daily-transaction-summary-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CBADailyTransactionSummaryReportResponse](#schemav2cbadailytransactionsummaryreportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get interest and fee report

<a id="opIdv2GetInterestFeeReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/interest-fees HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/interest-fees`

Returns the standard CBA interest and fee summary for accounts in scope. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-interest-and-fee-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|client_id|query|string(uuid)|false|Restrict the report to a client.|
|account_id|query|string(uuid)|false|Restrict the report to a single account.|
|startTime|query|string(date-time)|false|none|
|endTime|query|string(date-time)|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "accounts": [
      {
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "account_number": "string",
        "currency": "string",
        "interest_accrued": "2.50",
        "interest_posted_count": 0,
        "interest_pending_count": 0,
        "fee_total": "10.00",
        "fee_posted_amount": "7.50",
        "fee_pending_recovery_amount": "2.50",
        "fee_posted_count": 0,
        "fee_pending_recovery_count": 0
      }
    ],
    "totals": {
      "interest_accrued": "2.50",
      "interest_posted_count": 0,
      "interest_pending_count": 0,
      "fee_total": "10.00",
      "fee_posted_amount": "7.50",
      "fee_pending_recovery_amount": "2.50",
      "fee_posted_count": 0,
      "fee_pending_recovery_count": 0
    }
  }
}
```

<h3 id="get-interest-and-fee-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2CBAInterestFeeReportResponse](#schemav2cbainterestfeereportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get trial balance report

<a id="opIdv2GetFinanceTrialBalanceReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/finance/trial-balance?currency=USD HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/finance/trial-balance`

Returns a finance trial balance snapshot across the LedgerTrack CBA ledgers for a single currency. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-trial-balance-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|currency|query|string|true|none|
|asOf|query|string(date-time)|false|none|
|fee_income_account|query|string|false|none|
|interest_expense_account|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "currency": "string",
    "as_of": "2019-08-24T14:15:22Z",
    "lines": [
      {
        "ledger": "string",
        "account": "string",
        "balance": 0,
        "currency": "string"
      }
    ]
  }
}
```

<h3 id="get-trial-balance-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FinanceTrialBalanceReportResponse](#schemav2financetrialbalancereportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get the GL registry trial balance

<a id="opIdv2GetGLTrialBalanceReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/finance/gl-trial-balance?currency=NGN HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/finance/gl-trial-balance`

Lists every active GL account of a currency with its debit or credit balance, per-type subtotals and a `balanced` flag (total debits equal total credits). Unlike `/reports/finance/trial-balance` it is driven by the GL registry, not by a fixed set of lines.
Sign convention: ledger balance is input - output; a positive balance is a debit and a negative balance a credit. A ledger that does not exist yet counts as zero. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-the-gl-registry-trial-balance-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|currency|query|string|true|none|
|asOf|query|string(date-time)|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "currency": "string",
    "asset": "string",
    "as_of": "2019-08-24T14:15:22Z",
    "accounts": [
      {
        "code": "string",
        "name": "string",
        "type": "asset",
        "normal_side": "debit",
        "currency": "string",
        "parent_code": "string",
        "ledger": "string",
        "address": "string",
        "status": "active",
        "asset": "NGN/2",
        "pit": "2019-08-24T14:15:22Z",
        "balance": 0,
        "debit": 0,
        "credit": 0,
        "natural_balance": 0,
        "abnormal": true
      }
    ],
    "subtotals": [
      {
        "type": "asset",
        "accounts": 0,
        "debit": 0,
        "credit": 0,
        "net": 0
      }
    ],
    "total_debits": 0,
    "total_credits": 0,
    "difference": 0,
    "balanced": true
  }
}
```

<h3 id="get-the-gl-registry-trial-balance-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2GLTrialBalanceReportResponse](#schemav2gltrialbalancereportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get balance sheet report

<a id="opIdv2GetFinanceBalanceSheetReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/finance/balance-sheet?currency=USD HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/finance/balance-sheet`

Returns a simplified balance sheet snapshot across the LedgerTrack CBA ledgers for a single currency. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-balance-sheet-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|currency|query|string|true|none|
|asOf|query|string(date-time)|false|none|
|fee_income_account|query|string|false|none|
|interest_expense_account|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "currency": "string",
    "as_of": "2019-08-24T14:15:22Z",
    "assets": [
      {
        "name": "string",
        "balance": 0,
        "currency": "string"
      }
    ],
    "liabilities": [
      {
        "name": "string",
        "balance": 0,
        "currency": "string"
      }
    ],
    "net_position": 0
  }
}
```

<h3 id="get-balance-sheet-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FinanceBalanceSheetReportResponse](#schemav2financebalancesheetreportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get profit and loss report

<a id="opIdv2GetFinanceProfitAndLossReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/finance/pnl?currency=USD&startTime=2019-08-24T14%3A15%3A22Z&endTime=2019-08-24T14%3A15%3A22Z HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/finance/pnl`

Returns a simplified profit and loss report for a single currency. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-profit-and-loss-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|currency|query|string|true|none|
|startTime|query|string(date-time)|true|none|
|endTime|query|string(date-time)|true|none|
|fee_income_account|query|string|false|none|
|interest_expense_account|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "currency": "string",
    "start_time": "2019-08-24T14:15:22Z",
    "end_time": "2019-08-24T14:15:22Z",
    "income": [
      {
        "name": "string",
        "amount": 0,
        "currency": "string"
      }
    ],
    "expenses": [
      {
        "name": "string",
        "amount": 0,
        "currency": "string"
      }
    ],
    "net_profit": 0
  }
}
```

<h3 id="get-profit-and-loss-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FinanceProfitAndLossReportResponse](#schemav2financeprofitandlossreportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get cash flow report

<a id="opIdv2GetFinanceCashFlowReport"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reports/finance/cash-flow?currency=USD&startTime=2019-08-24T14%3A15%3A22Z&endTime=2019-08-24T14%3A15%3A22Z HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reports/finance/cash-flow`

Returns a simplified cash flow report for a single currency. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-cash-flow-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|currency|query|string|true|none|
|startTime|query|string(date-time)|true|none|
|endTime|query|string(date-time)|true|none|
|fee_income_account|query|string|false|none|
|interest_expense_account|query|string|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "currency": "string",
    "start_time": "2019-08-24T14:15:22Z",
    "end_time": "2019-08-24T14:15:22Z",
    "lines": [
      {
        "name": "string",
        "opening": 0,
        "closing": 0,
        "net_change": 0,
        "currency": "string"
      }
    ]
  }
}
```

<h3 id="get-cash-flow-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2FinanceCashFlowReportResponse](#schemav2financecashflowreportresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## List reserve holds

<a id="opIdv2ListReserveHolds"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reserve-holds HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reserve-holds`

Lists the shares of credits held in wallet liens under a reserve rule, oldest release first. A hold is `pending` while its credit is being posted, `held` until `release_at`, then `released`. `voided` means the credit never reached the ledger; `needs_review` means the release failed repeatedly (for example the lien was spent) and needs an operator. `due_before` keeps holds whose release time is at or before it. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="list-reserve-holds-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|account_id|query|string(uuid)|false|none|
|client_id|query|string(uuid)|false|none|
|wallet_id|query|string|false|none|
|status|query|string|false|none|
|due_before|query|string(date-time)|false|RFC 3339 time.|
|limit|query|integer|false|1 to 500.|

#### Enumerated Values

|Parameter|Value|
|---|---|
|status|pending|
|status|held|
|status|released|
|status|voided|
|status|needs_review|

> Example responses

> 200 Response

```json
{
  "data": {
    "holds": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "ledger": "string",
        "wallet_id": "string",
        "currency": "string",
        "credit_reference": "string",
        "credit_tx_id": 0,
        "credit_type": "string",
        "credit_amount_minor": 0,
        "amount_minor": 0,
        "release_at": "2019-08-24T14:15:22Z",
        "status": "pending",
        "released_at": "2019-08-24T14:15:22Z",
        "release_reason": "sla",
        "release_reference": "string",
        "attempts": 0,
        "last_error": "string",
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="list-reserve-holds-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ReserveHoldListResponse](#schemav2reserveholdlistresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get a reserve hold

<a id="opIdv2GetReserveHold"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/reserve-holds/{id} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/reserve-holds/{id}`

This endpoint is supported on the `ledgertrack` ledger.

<h3 id="get-a-reserve-hold-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "ledger": "string",
    "wallet_id": "string",
    "currency": "string",
    "credit_reference": "string",
    "credit_tx_id": 0,
    "credit_type": "string",
    "credit_amount_minor": 0,
    "amount_minor": 0,
    "release_at": "2019-08-24T14:15:22Z",
    "status": "pending",
    "released_at": "2019-08-24T14:15:22Z",
    "release_reason": "sla",
    "release_reference": "string",
    "attempts": 0,
    "last_error": "string",
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-a-reserve-hold-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ReserveHoldResponse](#schemav2reserveholdresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Release a reserve hold now

<a id="opIdv2ReleaseReserveHold"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reserve-holds/{id}/release HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/reserve-holds/{id}/release`

Moves the held share from the wallet lien to the available balance ahead of its SLA. Releasing a hold that is already released returns it unchanged and moves nothing. A hold that is `pending`, `voided`, or leased by the release job answers 409. This endpoint is supported on the `ledgertrack` ledger.

<h3 id="release-a-reserve-hold-now-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger. The GL registry is served on `ledgertrack`.|
|id|path|string(uuid)|true|Identifier of the fee override or tax rule.|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "ledger": "string",
    "wallet_id": "string",
    "currency": "string",
    "credit_reference": "string",
    "credit_tx_id": 0,
    "credit_type": "string",
    "credit_amount_minor": 0,
    "amount_minor": 0,
    "release_at": "2019-08-24T14:15:22Z",
    "status": "pending",
    "released_at": "2019-08-24T14:15:22Z",
    "release_reason": "sla",
    "release_reference": "string",
    "attempts": 0,
    "last_error": "string",
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="release-a-reserve-hold-now-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2ReserveHoldResponse](#schemav2reserveholdresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Credit a wallet

<a id="opIdv2CreditWallet"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/wallets/{walletID}/credit HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/wallets/{walletID}/credit`

On the `ledgertrack` ledger, when a reserve rule applies to the wallet's account, the credit is posted as one transaction that puts the reserve share in the wallet lien and the rest in the available balance. The response then carries `reserve` with `hold_id`, `reserve_minor`, `available_minor` and `release_at`. `credit_type` selects rules that list credit types. A retry of the same `reference` keeps the original split. Without a matching rule the credit is posted as before.

> Body parameter

```json
{
  "amount": 0,
  "reference": "string",
  "credit_type": "string",
  "metadata": {
    "property1": "string",
    "property2": "string"
  }
}
```

<h3 id="credit-a-wallet-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|walletID|path|string|true|Wallet ID|
|Idempotency-Key|header|string|false|Use an idempotency key|
|body|body|[V2CreditWalletRequest](#schemav2creditwalletrequest)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}
```

<h3 id="credit-a-wallet-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Wallet credited|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Debit a wallet

<a id="opIdv2DebitWallet"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/wallets/{walletID}/debit HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/wallets/{walletID}/debit`

With `draw_from_reserve` set, on the `ledgertrack` ledger, the debit spends the wallet's held reserve shares before the available balance, oldest release first. This is for chargebacks. The held shares are the wallet lien, so the debit takes the drawn part from the lien and the rest from available in one transaction, and the holds shrink by the same amount (a hold used up entirely shows `released` with reason `drawn`). The response carries `reserve` with `drawn_minor` and `available_minor`. A retry of the same `reference` draws once. If the debit is refused for lack of funds the holds are restored. It cannot be combined with `channelID`.

> Body parameter

```json
{
  "amount": 0,
  "reference": "string",
  "metadata": {
    "property1": "string",
    "property2": "string"
  },
  "channelID": "string",
  "channelAmount": 0,
  "draw_from_reserve": true
}
```

<h3 id="debit-a-wallet-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|walletID|path|string|true|Wallet ID|
|Idempotency-Key|header|string|false|Use an idempotency key|
|body|body|[V2DebitWalletRequest](#schemav2debitwalletrequest)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}
```

<h3 id="debit-a-wallet-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Wallet debited|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Lien a wallet

<a id="opIdv2LienWallet"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/wallets/{walletID}/lien HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/wallets/{walletID}/lien`

> Body parameter

```json
false
```

<h3 id="lien-a-wallet-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|walletID|path|string|true|Wallet ID|
|Idempotency-Key|header|string|false|Use an idempotency key|

> Example responses

> 201 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}
```

<h3 id="lien-a-wallet-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Wallet liened|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Release a lien

<a id="opIdv2ReleaseLien"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/wallets/{walletID}/lien/release HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json
Idempotency-Key: string

```

`POST /v2/{ledger}/wallets/{walletID}/lien/release`

> Body parameter

```json
false
```

<h3 id="release-a-lien-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|walletID|path|string|true|Wallet ID|
|Idempotency-Key|header|string|false|Use an idempotency key|

> Example responses

> 201 Response

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}
```

<h3 id="release-a-lien-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Lien released|[V2CreateTransactionResponse](#schemav2createtransactionresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Get wallet statement

<a id="opIdv2GetWalletStatement"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/wallets/{walletID}/statement HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/wallets/{walletID}/statement`

<h3 id="get-wallet-statement-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|walletID|path|string|true|Wallet ID|
|startTime|query|string(date-time)|false|none|
|endTime|query|string(date-time)|false|none|
|reference|query|string|false|none|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests.|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "insertedAt": "2019-08-24T14:15:22Z",
        "updatedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "template": "string"
      }
    ]
  }
}
```

<h3 id="get-wallet-statement-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2TransactionsCursorResponse](#schemav2transactionscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Get wallet history

<a id="opIdv2GetWalletHistory"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/wallets/{walletID}/history HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/wallets/{walletID}/history`

<h3 id="get-wallet-history-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|walletID|path|string|true|Wallet ID|
|startTime|query|string(date-time)|false|none|
|endTime|query|string(date-time)|false|none|
|reference|query|string|false|none|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests.|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "insertedAt": "2019-08-24T14:15:22Z",
        "updatedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "template": "string"
      }
    ]
  }
}
```

<h3 id="get-wallet-history-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2TransactionsCursorResponse](#schemav2transactionscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## List the logs from a ledger

<a id="opIdv2ListLogs"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/logs HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`GET /v2/{ledger}/logs`

List the logs from a ledger, sorted by ID in descending order.

> Body parameter

```json
{}
```

<h3 id="list-the-logs-from-a-ledger-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pageSize|query|integer(int64)|false|The maximum number of results to return per page.|
|cursor|query|string|false|Parameter used in pagination requests. Maximum page size is set to 15.|
|pit|query|string(date-time)|false|none|
|sort|query|string|false|Sort results using a field name and order (ascending or descending). |
|body|body|object|true|none|

#### Detailed descriptions

**pageSize**: The maximum number of results to return per page.

**cursor**: Parameter used in pagination requests. Maximum page size is set to 15.
Set to the value of next for the next page of results.
Set to the value of previous for the previous page of results.
No other parameters can be set when this parameter is set.

**sort**: Sort results using a field name and order (ascending or descending). 
Format: `<field>:<order>`, where `<field>` is the field name and `<order>` is either `asc` or `desc`.

> Example responses

> 200 Response

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "id": 1234,
        "type": "NEW_TRANSACTION",
        "data": {
          "transaction": {
            "id": 1234,
            "postings": [
              {
                "source": "world",
                "destination": "users:001",
                "amount": 100,
                "asset": "USD/2"
              }
            ],
            "metadata": {},
            "timestamp": "2024-01-15T10:30:00Z",
            "insertedAt": "2024-01-15T10:30:00Z",
            "reverted": false
          },
          "accountMetadata": {
            "users:001": {
              "created_by": "system"
            }
          }
        },
        "hash": "9ee060170400f556b7e1575cb13f9db004f150a08355c7431c62bc639166431e",
        "date": "2019-08-24T14:15:22Z",
        "schemaVersion": "v1.0.0"
      }
    ]
  }
}
```

<h3 id="list-the-logs-from-a-ledger-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|[V2LogsCursorResponse](#schemav2logscursorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## v2ImportLogs

<a id="opIdv2ImportLogs"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/logs/import HTTP/1.1
Host: localhost:8080
Content-Type: application/octet-stream
Accept: application/json

```

`POST /v2/{ledger}/logs/import`

> Body parameter

```yaml
file: string

```

<h3 id="v2importlogs-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|body|body|[V2ImportLogsRequest](#schemav2importlogsrequest)|true|none|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="v2importlogs-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Import OK|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Export logs

<a id="opIdv2ExportLogs"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/logs/export HTTP/1.1
Host: localhost:8080
Accept: application/octet-stream

```

`POST /v2/{ledger}/logs/export`

<h3 id="export-logs-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> default Response

<h3 id="export-logs-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Import OK|None|
|default|Default|Error|string|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List exporters

<a id="opIdv2ListExporters"></a>

> Code samples

```http
GET http://localhost:8080/v2/_/exporters HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/_/exporters`

> Example responses

> 200 Response

```json
{
  "cursor": {
    "cursor": {
      "pageSize": 15,
      "hasMore": false,
      "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
      "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
      "data": [
        {
          "driver": "string",
          "config": {},
          "id": "string",
          "createdAt": "2019-08-24T14:15:22Z"
        }
      ]
    },
    "data": [
      {
        "driver": "string",
        "config": {},
        "id": "string",
        "createdAt": "2019-08-24T14:15:22Z"
      }
    ]
  }
}
```

<h3 id="list-exporters-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Exporters list|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="list-exporters-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» cursor|any|false|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|[V2ExportersCursorResponse](#schemav2exporterscursorresponse)|false|none|none|
|»»» cursor|object|true|none|none|
|»»»» pageSize|integer(int64)|true|none|none|
|»»»» hasMore|boolean|true|none|none|
|»»»» previous|string|false|none|none|
|»»»» next|string|false|none|none|
|»»»» data|[allOf]|true|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»»»»» *anonymous*|[V2ExporterConfiguration](#schemav2exporterconfiguration)|false|none|none|
|»»»»»» driver|string|true|none|none|
|»»»»»» config|object|true|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»»»»» *anonymous*|object|false|none|none|
|»»»»»» id|string|true|none|none|
|»»»»»» createdAt|string(date-time)|true|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» data|[allOf]|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Create exporter

<a id="opIdv2CreateExporter"></a>

> Code samples

```http
POST http://localhost:8080/v2/_/exporters HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/_/exporters`

> Body parameter

```json
{
  "driver": "string",
  "config": {}
}
```

<h3 id="create-exporter-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[V2ExporterConfiguration](#schemav2exporterconfiguration)|true|none|

> Example responses

> 201 Response

```json
{
  "data": {
    "driver": "string",
    "config": {},
    "id": "string",
    "createdAt": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="create-exporter-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Created exporter|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="create-exporter-responseschema">Response Schema</h3>

Status Code **201**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|[V2Exporter](#schemav2exporter)|true|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|[V2ExporterConfiguration](#schemav2exporterconfiguration)|false|none|none|
|»»» driver|string|true|none|none|
|»»» config|object|true|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» id|string|true|none|none|
|»»» createdAt|string(date-time)|true|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Get exporter state

<a id="opIdv2GetExporterState"></a>

> Code samples

```http
GET http://localhost:8080/v2/_/exporters/{exporterID} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/_/exporters/{exporterID}`

<h3 id="get-exporter-state-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|exporterID|path|string|true|The exporter id|

> Example responses

> 200 Response

```json
{
  "data": {
    "driver": "string",
    "config": {},
    "id": "string",
    "createdAt": "2019-08-24T14:15:22Z"
  }
}
```

<h3 id="get-exporter-state-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Exporter information|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="get-exporter-state-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|[V2Exporter](#schemav2exporter)|true|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|[V2ExporterConfiguration](#schemav2exporterconfiguration)|false|none|none|
|»»» driver|string|true|none|none|
|»»» config|object|true|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» id|string|true|none|none|
|»»» createdAt|string(date-time)|true|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Update exporter

<a id="opIdv2UpdateExporter"></a>

> Code samples

```http
PUT http://localhost:8080/v2/_/exporters/{exporterID} HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`PUT /v2/_/exporters/{exporterID}`

> Body parameter

```json
{
  "driver": "string",
  "config": {}
}
```

<h3 id="update-exporter-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[V2ExporterConfiguration](#schemav2exporterconfiguration)|true|none|
|exporterID|path|string|true|The exporter id|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="update-exporter-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Exporter updated|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Delete exporter

<a id="opIdv2DeleteExporter"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/_/exporters/{exporterID} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`DELETE /v2/_/exporters/{exporterID}`

<h3 id="delete-exporter-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|exporterID|path|string|true|The exporter id|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="delete-exporter-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Exporter deleted|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## Delete bucket

<a id="opIdv2DeleteBucket"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/_/buckets/{bucket} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`DELETE /v2/_/buckets/{bucket}`

Delete a bucket by marking all ledgers in the bucket as deleted (soft delete). All ledgers in the bucket will have their deleted_at field set to the current timestamp.

<h3 id="delete-bucket-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|bucket|path|string|true|The bucket name|

> Example responses

> 404 Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="delete-bucket-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Bucket deleted|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Bucket not found|[V2ErrorResponse](#schemav2errorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## Restore bucket

<a id="opIdv2RestoreBucket"></a>

> Code samples

```http
POST http://localhost:8080/v2/_/buckets/{bucket}/restore HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/_/buckets/{bucket}/restore`

Restore a deleted bucket by unmarking all ledgers in the bucket as deleted. All ledgers in the bucket will have their deleted_at field set to NULL.

<h3 id="restore-bucket-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|bucket|path|string|true|The bucket name|

> Example responses

> 404 Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="restore-bucket-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Bucket restored|None|
|404|[Not Found](https://tools.ietf.org/html/rfc7231#section-6.5.4)|Bucket not found|[V2ErrorResponse](#schemav2errorresponse)|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List pipelines

<a id="opIdv2ListPipelines"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/pipelines HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/pipelines`

<h3 id="list-pipelines-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{
  "cursor": {
    "cursor": {
      "pageSize": 15,
      "hasMore": false,
      "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
      "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
      "data": [
        {
          "id": "string",
          "createdAt": "2019-08-24T14:15:22Z",
          "lastLogID": 0,
          "enabled": true
        }
      ]
    },
    "data": [
      {
        "id": "string",
        "createdAt": "2019-08-24T14:15:22Z",
        "lastLogID": 0,
        "enabled": true
      }
    ]
  }
}
```

<h3 id="list-pipelines-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Pipelines list|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="list-pipelines-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» cursor|any|false|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|[V2PipelinesCursorResponse](#schemav2pipelinescursorresponse)|false|none|none|
|»»» cursor|object|true|none|none|
|»»»» pageSize|integer(int64)|true|none|none|
|»»»» hasMore|boolean|true|none|none|
|»»»» previous|string|false|none|none|
|»»»» next|string|false|none|none|
|»»»» data|[allOf]|true|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»»»»» *anonymous*|object|false|none|none|
|»»»»»» ledger|string|true|none|none|
|»»»»»» exporterID|string|true|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»»»»» *anonymous*|object|false|none|none|
|»»»»»» id|string|true|none|none|
|»»»»»» createdAt|string(date-time)|true|none|none|
|»»»»»» lastLogID|integer|false|none|none|
|»»»»»» enabled|boolean|false|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» data|[allOf]|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Create pipeline

<a id="opIdv2CreatePipeline"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/pipelines HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/pipelines`

> Body parameter

```json
{
  "exporterID": "string"
}
```

<h3 id="create-pipeline-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|[V2CreatePipelineRequest](#schemav2createpipelinerequest)|false|none|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 201 Response

```json
{
  "data": {
    "id": "string",
    "createdAt": "2019-08-24T14:15:22Z",
    "lastLogID": 0,
    "enabled": true
  }
}
```

<h3 id="create-pipeline-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|201|[Created](https://tools.ietf.org/html/rfc7231#section-6.3.2)|Created ipeline|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="create-pipeline-responseschema">Response Schema</h3>

Status Code **201**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|any|true|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» ledger|string|true|none|none|
|»»» exporterID|string|true|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» id|string|true|none|none|
|»»» createdAt|string(date-time)|true|none|none|
|»»» lastLogID|integer|false|none|none|
|»»» enabled|boolean|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Get pipeline state

<a id="opIdv2GetPipelineState"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/pipelines/{pipelineID} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/pipelines/{pipelineID}`

<h3 id="get-pipeline-state-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pipelineID|path|string|true|The pipeline id|

> Example responses

> 200 Response

```json
{
  "data": {
    "id": "string",
    "createdAt": "2019-08-24T14:15:22Z",
    "lastLogID": 0,
    "enabled": true
  }
}
```

<h3 id="get-pipeline-state-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|Pipeline information|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="get-pipeline-state-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|any|true|none|none|

*allOf*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» ledger|string|true|none|none|
|»»» exporterID|string|true|none|none|

*and*

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|»» *anonymous*|object|false|none|none|
|»»» id|string|true|none|none|
|»»» createdAt|string(date-time)|true|none|none|
|»»» lastLogID|integer|false|none|none|
|»»» enabled|boolean|false|none|none|

<aside class="success">
This operation does not require authentication
</aside>

## Delete pipeline

<a id="opIdv2DeletePipeline"></a>

> Code samples

```http
DELETE http://localhost:8080/v2/{ledger}/pipelines/{pipelineID} HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`DELETE /v2/{ledger}/pipelines/{pipelineID}`

<h3 id="delete-pipeline-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pipelineID|path|string|true|The pipeline id|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="delete-pipeline-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|204|[No Content](https://tools.ietf.org/html/rfc7231#section-6.3.5)|Pipeline deleted|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## Reset pipeline

<a id="opIdv2ResetPipeline"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/pipelines/{pipelineID}/reset HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/pipelines/{pipelineID}/reset`

<h3 id="reset-pipeline-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pipelineID|path|string|true|The pipeline id|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="reset-pipeline-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|Pipeline reset|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## Start pipeline

<a id="opIdv2StartPipeline"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/pipelines/{pipelineID}/start HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/pipelines/{pipelineID}/start`

<h3 id="start-pipeline-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pipelineID|path|string|true|The pipeline id|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="start-pipeline-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|Pipeline started|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## Stop pipeline

<a id="opIdv2StopPipeline"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/pipelines/{pipelineID}/stop HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`POST /v2/{ledger}/pipelines/{pipelineID}/stop`

<h3 id="stop-pipeline-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|pipelineID|path|string|true|The pipeline id|

> Example responses

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="stop-pipeline-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|202|[Accepted](https://tools.ietf.org/html/rfc7231#section-6.3.3)|Pipeline stopped|None|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="success">
This operation does not require authentication
</aside>

## Get channel fee configuration

<a id="opIdv2GetChannelFeeConfig"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/channels/{channelID}/fees/config HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/channels/{channelID}/fees/config`

<h3 id="get-channel-fee-configuration-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|channelID|path|string|true|Channel identifier.|

> Example responses

> 200 Response

```json
{
  "data": {}
}
```

<h3 id="get-channel-fee-configuration-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="get-channel-fee-configuration-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|object|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Upsert channel fee configuration

<a id="opIdv2UpsertChannelFeeConfig"></a>

> Code samples

```http
PUT http://localhost:8080/v2/{ledger}/channels/{channelID}/fees/config HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`PUT /v2/{ledger}/channels/{channelID}/fees/config`

> Body parameter

```json
{
  "currency": "string",
  "enabled": true,
  "actor": "string",
  "user_fee": {},
  "processing_fee": {}
}
```

<h3 id="upsert-channel-fee-configuration-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|object|true|none|
|» currency|body|string|true|none|
|» enabled|body|boolean|false|none|
|» actor|body|string|false|none|
|» user_fee|body|object|true|none|
|» processing_fee|body|object|true|none|
|ledger|path|string|true|Name of the ledger.|
|channelID|path|string|true|Channel identifier.|

> Example responses

> 200 Response

```json
{
  "data": {}
}
```

<h3 id="upsert-channel-fee-configuration-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="upsert-channel-fee-configuration-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|object|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:write )
</aside>

## List channel fee configuration audit entries

<a id="opIdv2ListChannelFeeConfigAudits"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/channels/{channelID}/fees/audits HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/channels/{channelID}/fees/audits`

<h3 id="list-channel-fee-configuration-audit-entries-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|channelID|path|string|true|Channel identifier.|
|limit|query|integer(int64)|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "audits": [
      {}
    ]
  }
}
```

<h3 id="list-channel-fee-configuration-audit-entries-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="list-channel-fee-configuration-audit-entries-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|object|true|none|none|
|»» audits|[object]|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## List channel fee configurations

<a id="opIdv2ListChannelFeeConfigs"></a>

> Code samples

```http
GET http://localhost:8080/v2/{ledger}/channels/fees/configs HTTP/1.1
Host: localhost:8080
Accept: application/json

```

`GET /v2/{ledger}/channels/fees/configs`

<h3 id="list-channel-fee-configurations-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|ledger|path|string|true|Name of the ledger.|
|currency|query|string|false|none|
|enabled|query|boolean|false|none|

> Example responses

> 200 Response

```json
{
  "data": {
    "configs": [
      {}
    ]
  }
}
```

<h3 id="list-channel-fee-configurations-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="list-channel-fee-configurations-responseschema">Response Schema</h3>

Status Code **200**

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» data|object|true|none|none|
|»» configs|[object]|true|none|none|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Channel revenue summary report

<a id="opIdv2GetChannelRevenueSummary"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reports/channels/revenue HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/reports/channels/revenue`

> Body parameter

```json
{
  "currency": "string",
  "channel_id": "string",
  "start_time": "2019-08-24T14:15:22Z",
  "end_time": "2019-08-24T14:15:22Z",
  "limit": 0,
  "offset": 0
}
```

<h3 id="channel-revenue-summary-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|object|true|none|
|» currency|body|string|true|none|
|» channel_id|body|string|false|none|
|» start_time|body|string(date-time)|false|none|
|» end_time|body|string(date-time)|false|none|
|» limit|body|integer(int64)|false|none|
|» offset|body|integer(int64)|false|none|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{}
```

<h3 id="channel-revenue-summary-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="channel-revenue-summary-report-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Channel revenue timeseries report

<a id="opIdv2GetChannelRevenueTimeseries"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reports/channels/revenue/timeseries HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/reports/channels/revenue/timeseries`

> Body parameter

```json
{
  "currency": "string",
  "channel_id": "string",
  "interval": "day",
  "start_time": "2019-08-24T14:15:22Z",
  "end_time": "2019-08-24T14:15:22Z"
}
```

<h3 id="channel-revenue-timeseries-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|object|true|none|
|» currency|body|string|true|none|
|» channel_id|body|string|false|none|
|» interval|body|string|false|none|
|» start_time|body|string(date-time)|true|none|
|» end_time|body|string(date-time)|true|none|
|ledger|path|string|true|Name of the ledger.|

#### Enumerated Values

|Parameter|Value|
|---|---|
|» interval|day|
|» interval|week|
|» interval|month|

> Example responses

> 200 Response

```json
{}
```

<h3 id="channel-revenue-timeseries-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="channel-revenue-timeseries-report-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Export channel revenue report

<a id="opIdv2ExportChannelRevenueReport"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reports/channels/revenue/export HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: text/csv

```

`POST /v2/{ledger}/reports/channels/revenue/export`

> Body parameter

```json
{
  "currency": "string",
  "format": "csv",
  "report": "summary",
  "channel_id": "string",
  "start_time": "2019-08-24T14:15:22Z",
  "end_time": "2019-08-24T14:15:22Z",
  "interval": "day",
  "limit": 0,
  "offset": 0
}
```

<h3 id="export-channel-revenue-report-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|object|true|none|
|» currency|body|string|true|none|
|» format|body|string|false|none|
|» report|body|string|false|none|
|» channel_id|body|string|false|none|
|» start_time|body|string(date-time)|false|none|
|» end_time|body|string(date-time)|false|none|
|» interval|body|string|false|none|
|» limit|body|integer(int64)|false|none|
|» offset|body|integer(int64)|false|none|
|ledger|path|string|true|Name of the ledger.|

#### Enumerated Values

|Parameter|Value|
|---|---|
|» format|csv|
|» format|pdf|
|» report|summary|
|» report|timeseries|
|» report|records|
|» interval|day|
|» interval|week|
|» interval|month|

> Example responses

> 200 Response

> default Response

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}
```

<h3 id="export-channel-revenue-report-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|string|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

## Channel revenue dashboard metrics

<a id="opIdv2GetChannelRevenueDashboard"></a>

> Code samples

```http
POST http://localhost:8080/v2/{ledger}/reports/channels/dashboard HTTP/1.1
Host: localhost:8080
Content-Type: application/json
Accept: application/json

```

`POST /v2/{ledger}/reports/channels/dashboard`

> Body parameter

```json
{
  "currency": "string",
  "top_n": 0,
  "months": 0
}
```

<h3 id="channel-revenue-dashboard-metrics-parameters">Parameters</h3>

|Name|In|Type|Required|Description|
|---|---|---|---|---|
|body|body|object|true|none|
|» currency|body|string|true|none|
|» top_n|body|integer(int64)|false|none|
|» months|body|integer(int64)|false|none|
|ledger|path|string|true|Name of the ledger.|

> Example responses

> 200 Response

```json
{}
```

<h3 id="channel-revenue-dashboard-metrics-responses">Responses</h3>

|Status|Meaning|Description|Schema|
|---|---|---|---|
|200|[OK](https://tools.ietf.org/html/rfc7231#section-6.3.1)|OK|Inline|
|default|Default|Error|[V2ErrorResponse](#schemav2errorresponse)|

<h3 id="channel-revenue-dashboard-metrics-responseschema">Response Schema</h3>

<aside class="warning">
To perform this operation, you must be authenticated by means of one of the following methods:
Authorization ( Scopes: ledger:read )
</aside>

# Schemas

<h2 id="tocS_V2ExportersCursorResponse">V2ExportersCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2exporterscursorresponse"></a>
<a id="schema_V2ExportersCursorResponse"></a>
<a id="tocSv2exporterscursorresponse"></a>
<a id="tocsv2exporterscursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "driver": "string",
        "config": {},
        "id": "string",
        "createdAt": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Exporter](#schemav2exporter)]|true|none|none|

<h2 id="tocS_V2PipelinesCursorResponse">V2PipelinesCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2pipelinescursorresponse"></a>
<a id="schema_V2PipelinesCursorResponse"></a>
<a id="tocSv2pipelinescursorresponse"></a>
<a id="tocsv2pipelinescursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "id": "string",
        "createdAt": "2019-08-24T14:15:22Z",
        "lastLogID": 0,
        "enabled": true
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Pipeline](#schemav2pipeline)]|true|none|none|

<h2 id="tocS_V2AccountsCursorResponse">V2AccountsCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2accountscursorresponse"></a>
<a id="schema_V2AccountsCursorResponse"></a>
<a id="tocSv2accountscursorresponse"></a>
<a id="tocsv2accountscursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "address": "users:001",
        "metadata": {
          "admin": "true"
        },
        "insertionDate": "2023-01-01T00:00:00Z",
        "updatedAt": "2023-01-01T00:00:00Z",
        "firstUsage": "2023-01-01T00:00:00Z",
        "volumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        },
        "effectiveVolumes": {
          "USD": {
            "input": 100,
            "output": 10,
            "balance": 90
          },
          "EUR": {
            "input": 100,
            "output": 10,
            "balance": 90
          }
        }
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Account](#schemav2account)]|true|none|none|

<h2 id="tocS_V2TransactionsCursorResponse">V2TransactionsCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2transactionscursorresponse"></a>
<a id="schema_V2TransactionsCursorResponse"></a>
<a id="tocSv2transactionscursorresponse"></a>
<a id="tocsv2transactionscursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "insertedAt": "2019-08-24T14:15:22Z",
        "updatedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "template": "string"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Transaction](#schemav2transaction)]|true|none|none|

<h2 id="tocS_V2LogsCursorResponse">V2LogsCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2logscursorresponse"></a>
<a id="schema_V2LogsCursorResponse"></a>
<a id="tocSv2logscursorresponse"></a>
<a id="tocsv2logscursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "id": 1234,
        "type": "NEW_TRANSACTION",
        "data": {
          "transaction": {
            "id": 1234,
            "postings": [
              {
                "source": "world",
                "destination": "users:001",
                "amount": 100,
                "asset": "USD/2"
              }
            ],
            "metadata": {},
            "timestamp": "2024-01-15T10:30:00Z",
            "insertedAt": "2024-01-15T10:30:00Z",
            "reverted": false
          },
          "accountMetadata": {
            "users:001": {
              "created_by": "system"
            }
          }
        },
        "hash": "9ee060170400f556b7e1575cb13f9db004f150a08355c7431c62bc639166431e",
        "date": "2019-08-24T14:15:22Z",
        "schemaVersion": "v1.0.0"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Log](#schemav2log)]|true|none|[Represents an immutable log entry in the ledger. Each log captures an atomic operation<br>with its full payload, enabling audit trails and event sourcing patterns.<br>The data field structure depends on the log type.<br>]|

<h2 id="tocS_V2AccountResponse">V2AccountResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2accountresponse"></a>
<a id="schema_V2AccountResponse"></a>
<a id="tocSv2accountresponse"></a>
<a id="tocsv2accountresponse"></a>

```json
{
  "data": {
    "address": "users:001",
    "metadata": {
      "admin": "true"
    },
    "insertionDate": "2023-01-01T00:00:00Z",
    "updatedAt": "2023-01-01T00:00:00Z",
    "firstUsage": "2023-01-01T00:00:00Z",
    "volumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "effectiveVolumes": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      },
      "EUR": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Account](#schemav2account)|true|none|none|

<h2 id="tocS_V2AggregateBalancesResponse">V2AggregateBalancesResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2aggregatebalancesresponse"></a>
<a id="schema_V2AggregateBalancesResponse"></a>
<a id="tocSv2aggregatebalancesresponse"></a>
<a id="tocsv2aggregatebalancesresponse"></a>

```json
{
  "data": {
    "USD": 100,
    "EUR": 12
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2AssetsBalances](#schemav2assetsbalances)|true|none|none|

<h2 id="tocS_V2VolumesWithBalanceCursorResponse">V2VolumesWithBalanceCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2volumeswithbalancecursorresponse"></a>
<a id="schema_V2VolumesWithBalanceCursorResponse"></a>
<a id="tocSv2volumeswithbalancecursorresponse"></a>
<a id="tocsv2volumeswithbalancecursorresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "account": "string",
        "asset": "string",
        "input": 0,
        "output": 0,
        "balance": 0
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2VolumesWithBalance](#schemav2volumeswithbalance)]|true|none|none|

<h2 id="tocS_V2VolumesWithBalance">V2VolumesWithBalance</h2>
<!-- backwards compatibility -->
<a id="schemav2volumeswithbalance"></a>
<a id="schema_V2VolumesWithBalance"></a>
<a id="tocSv2volumeswithbalance"></a>
<a id="tocsv2volumeswithbalance"></a>

```json
{
  "account": "string",
  "asset": "string",
  "input": 0,
  "output": 0,
  "balance": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account|string|true|none|none|
|asset|string|true|none|none|
|input|integer(bigint)|true|none|none|
|output|integer(bigint)|true|none|none|
|balance|integer(bigint)|true|none|none|

<h2 id="tocS_V2Metadata">V2Metadata</h2>
<!-- backwards compatibility -->
<a id="schemav2metadata"></a>
<a id="schema_V2Metadata"></a>
<a id="tocSv2metadata"></a>
<a id="tocsv2metadata"></a>

```json
{
  "admin": "true"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|string|false|none|none|

<h2 id="tocS_V2ConfigInfo">V2ConfigInfo</h2>
<!-- backwards compatibility -->
<a id="schemav2configinfo"></a>
<a id="schema_V2ConfigInfo"></a>
<a id="tocSv2configinfo"></a>
<a id="tocsv2configinfo"></a>

```json
{
  "server": "string",
  "version": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|server|string|true|none|none|
|version|string|true|none|none|

<h2 id="tocS_V2Account">V2Account</h2>
<!-- backwards compatibility -->
<a id="schemav2account"></a>
<a id="schema_V2Account"></a>
<a id="tocSv2account"></a>
<a id="tocsv2account"></a>

```json
{
  "address": "users:001",
  "metadata": {
    "admin": "true"
  },
  "insertionDate": "2023-01-01T00:00:00Z",
  "updatedAt": "2023-01-01T00:00:00Z",
  "firstUsage": "2023-01-01T00:00:00Z",
  "volumes": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    },
    "EUR": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  },
  "effectiveVolumes": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    },
    "EUR": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|address|string|true|none|none|
|metadata|object|true|none|none|
|» **additionalProperties**|string|false|none|none|
|insertionDate|string(date-time)|false|none|none|
|updatedAt|string(date-time)|false|none|none|
|firstUsage|string(date-time)|false|none|none|
|volumes|[V2Volumes](#schemav2volumes)|false|none|none|
|effectiveVolumes|[V2Volumes](#schemav2volumes)|false|none|none|

<h2 id="tocS_V2AssetsBalances">V2AssetsBalances</h2>
<!-- backwards compatibility -->
<a id="schemav2assetsbalances"></a>
<a id="schema_V2AssetsBalances"></a>
<a id="tocSv2assetsbalances"></a>
<a id="tocsv2assetsbalances"></a>

```json
{
  "USD": 100,
  "EUR": 12
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|integer(bigint)|false|none|none|

<h2 id="tocS_V2CurrenciesResponse">V2CurrenciesResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2currenciesresponse"></a>
<a id="schema_V2CurrenciesResponse"></a>
<a id="tocSv2currenciesresponse"></a>
<a id="tocsv2currenciesresponse"></a>

```json
{
  "data": {
    "currencies": [
      {
        "code": "USD",
        "precision": 2,
        "enabled": true
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» currencies|[[V2Currency](#schemav2currency)]|true|none|none|

<h2 id="tocS_V2Currency">V2Currency</h2>
<!-- backwards compatibility -->
<a id="schemav2currency"></a>
<a id="schema_V2Currency"></a>
<a id="tocSv2currency"></a>
<a id="tocsv2currency"></a>

```json
{
  "code": "USD",
  "precision": 2,
  "enabled": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|code|string|true|none|none|
|precision|integer|true|none|none|
|enabled|boolean|true|none|none|

<h2 id="tocS_V2CBAAddress">V2CBAAddress</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaaddress"></a>
<a id="schema_V2CBAAddress"></a>
<a id="tocSv2cbaaddress"></a>
<a id="tocsv2cbaaddress"></a>

```json
{
  "line1": "string",
  "line2": "string",
  "city": "string",
  "state": "string",
  "country": "string",
  "postal": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|line1|string|false|none|none|
|line2|string|false|none|none|
|city|string|false|none|none|
|state|string|false|none|none|
|country|string|false|none|none|
|postal|string|false|none|none|

<h2 id="tocS_V2CBAClientContact">V2CBAClientContact</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaclientcontact"></a>
<a id="schema_V2CBAClientContact"></a>
<a id="tocSv2cbaclientcontact"></a>
<a id="tocsv2cbaclientcontact"></a>

```json
{
  "email": "string",
  "phone": "string",
  "address": {
    "line1": "string",
    "line2": "string",
    "city": "string",
    "state": "string",
    "country": "string",
    "postal": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|email|string|false|none|none|
|phone|string|false|none|none|
|address|[V2CBAAddress](#schemav2cbaaddress)|false|none|none|

<h2 id="tocS_V2CBAIndividualData">V2CBAIndividualData</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaindividualdata"></a>
<a id="schema_V2CBAIndividualData"></a>
<a id="tocSv2cbaindividualdata"></a>
<a id="tocsv2cbaindividualdata"></a>

```json
{
  "first_name": "string",
  "middle_name": "string",
  "last_name": "string",
  "date_of_birth": "string",
  "gender": "string",
  "national_id_type": "string",
  "national_id_number": "string",
  "nationality": "string",
  "occupation": "string",
  "employer": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|first_name|string|false|none|none|
|middle_name|string|false|none|none|
|last_name|string|false|none|none|
|date_of_birth|string|false|none|none|
|gender|string|false|none|none|
|national_id_type|string|false|none|none|
|national_id_number|string|false|none|none|
|nationality|string|false|none|none|
|occupation|string|false|none|none|
|employer|string|false|none|none|

<h2 id="tocS_V2CBABeneficialOwner">V2CBABeneficialOwner</h2>
<!-- backwards compatibility -->
<a id="schemav2cbabeneficialowner"></a>
<a id="schema_V2CBABeneficialOwner"></a>
<a id="tocSv2cbabeneficialowner"></a>
<a id="tocsv2cbabeneficialowner"></a>

```json
{
  "name": "string",
  "ownership_pct": "string",
  "national_id": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|false|none|none|
|ownership_pct|string|false|none|none|
|national_id|string|false|none|none|

<h2 id="tocS_V2CBAAuthorizedSignatory">V2CBAAuthorizedSignatory</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaauthorizedsignatory"></a>
<a id="schema_V2CBAAuthorizedSignatory"></a>
<a id="tocSv2cbaauthorizedsignatory"></a>
<a id="tocsv2cbaauthorizedsignatory"></a>

```json
{
  "client_id": "string",
  "role": "string",
  "signing_limit": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|client_id|string|false|none|none|
|role|string|false|none|none|
|signing_limit|string|false|none|none|

<h2 id="tocS_V2CBACorporateData">V2CBACorporateData</h2>
<!-- backwards compatibility -->
<a id="schemav2cbacorporatedata"></a>
<a id="schema_V2CBACorporateData"></a>
<a id="tocSv2cbacorporatedata"></a>
<a id="tocsv2cbacorporatedata"></a>

```json
{
  "legal_name": "string",
  "trading_name": "string",
  "registration_number": "string",
  "tax_id": "string",
  "incorporation_date": "string",
  "incorporation_country": "string",
  "industry": "string",
  "beneficial_owners": [
    {
      "name": "string",
      "ownership_pct": "string",
      "national_id": "string"
    }
  ],
  "authorized_signatories": [
    {
      "client_id": "string",
      "role": "string",
      "signing_limit": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|legal_name|string|false|none|none|
|trading_name|string|false|none|none|
|registration_number|string|false|none|none|
|tax_id|string|false|none|none|
|incorporation_date|string|false|none|none|
|incorporation_country|string|false|none|none|
|industry|string|false|none|none|
|beneficial_owners|[[V2CBABeneficialOwner](#schemav2cbabeneficialowner)]|false|none|none|
|authorized_signatories|[[V2CBAAuthorizedSignatory](#schemav2cbaauthorizedsignatory)]|false|none|none|

<h2 id="tocS_V2CBAClient">V2CBAClient</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaclient"></a>
<a id="schema_V2CBAClient"></a>
<a id="tocSv2cbaclient"></a>
<a id="tocsv2cbaclient"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "client_number": "CL-2026-000123",
  "type": "individual",
  "status": "active",
  "kyc_level": 1,
  "kyc_status": "verified",
  "kyc_data": {},
  "contact": {
    "email": "string",
    "phone": "string",
    "address": {
      "line1": "string",
      "line2": "string",
      "city": "string",
      "state": "string",
      "country": "string",
      "postal": "string"
    }
  },
  "individual_data": {
    "first_name": "string",
    "middle_name": "string",
    "last_name": "string",
    "date_of_birth": "string",
    "gender": "string",
    "national_id_type": "string",
    "national_id_number": "string",
    "nationality": "string",
    "occupation": "string",
    "employer": "string"
  },
  "corporate_data": {
    "legal_name": "string",
    "trading_name": "string",
    "registration_number": "string",
    "tax_id": "string",
    "incorporation_date": "string",
    "incorporation_country": "string",
    "industry": "string",
    "beneficial_owners": [
      {
        "name": "string",
        "ownership_pct": "string",
        "national_id": "string"
      }
    ],
    "authorized_signatories": [
      {
        "client_id": "string",
        "role": "string",
        "signing_limit": "string"
      }
    ]
  },
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|client_number|string|true|none|none|
|type|string|true|none|none|
|status|string|true|none|none|
|kyc_level|integer|true|none|none|
|kyc_status|string|true|none|none|
|kyc_data|object|false|none|none|
|contact|[V2CBAClientContact](#schemav2cbaclientcontact)|true|none|none|
|individual_data|[V2CBAIndividualData](#schemav2cbaindividualdata)|false|none|none|
|corporate_data|[V2CBACorporateData](#schemav2cbacorporatedata)|false|none|none|
|created_at|string(date-time)|true|none|none|
|updated_at|string(date-time)|true|none|none|

<h2 id="tocS_V2CBAAccount">V2CBAAccount</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaaccount"></a>
<a id="schema_V2CBAAccount"></a>
<a id="tocSv2cbaaccount"></a>
<a id="tocsv2cbaaccount"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "account_number": "0123456789",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "USD",
  "status": "active",
  "wallet_id": "client-CL-2026-000123-SAV-USD-001",
  "freeze_debits": false,
  "opened_at": "2019-08-24T14:15:22Z",
  "activated_at": "2019-08-24T14:15:22Z",
  "closed_at": "2019-08-24T14:15:22Z",
  "last_activity_at": "2019-08-24T14:15:22Z",
  "interest_accrued": "2.50",
  "metadata": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|account_number|string|true|none|none|
|client_id|string(uuid)|true|none|none|
|product_id|string(uuid)|true|none|none|
|currency|string|true|none|none|
|status|string|true|none|none|
|wallet_id|string|true|none|none|
|freeze_debits|boolean|true|none|none|
|opened_at|string(date-time)|true|none|none|
|activated_at|string(date-time)|false|none|none|
|closed_at|string(date-time)|false|none|none|
|last_activity_at|string(date-time)|false|none|none|
|interest_accrued|string|true|none|none|
|metadata|object|false|none|none|

<h2 id="tocS_V2CBAAccountListResponse">V2CBAAccountListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaaccountlistresponse"></a>
<a id="schema_V2CBAAccountListResponse"></a>
<a id="tocSv2cbaaccountlistresponse"></a>
<a id="tocsv2cbaaccountlistresponse"></a>

```json
{
  "data": {
    "accounts": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "account_number": "0123456789",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "USD",
        "status": "active",
        "wallet_id": "client-CL-2026-000123-SAV-USD-001",
        "freeze_debits": false,
        "opened_at": "2019-08-24T14:15:22Z",
        "activated_at": "2019-08-24T14:15:22Z",
        "closed_at": "2019-08-24T14:15:22Z",
        "last_activity_at": "2019-08-24T14:15:22Z",
        "interest_accrued": "2.50",
        "metadata": {}
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» accounts|[[V2CBAAccount](#schemav2cbaaccount)]|true|none|none|

<h2 id="tocS_V2CBAClientPortfolioAccount">V2CBAClientPortfolioAccount</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaclientportfolioaccount"></a>
<a id="schema_V2CBAClientPortfolioAccount"></a>
<a id="tocSv2cbaclientportfolioaccount"></a>
<a id="tocsv2cbaclientportfolioaccount"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "account_number": "0123456789",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "USD",
  "status": "active",
  "wallet_id": "client-CL-2026-000123-SAV-USD-001",
  "freeze_debits": false,
  "opened_at": "2019-08-24T14:15:22Z",
  "activated_at": "2019-08-24T14:15:22Z",
  "closed_at": "2019-08-24T14:15:22Z",
  "last_activity_at": "2019-08-24T14:15:22Z",
  "interest_accrued": "2.50",
  "metadata": {},
  "available_balance": 10000
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2CBAAccount](#schemav2cbaaccount)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» available_balance|integer(int64)|true|none|none|

<h2 id="tocS_V2CBAClientPortfolioCurrencySummary">V2CBAClientPortfolioCurrencySummary</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaclientportfoliocurrencysummary"></a>
<a id="schema_V2CBAClientPortfolioCurrencySummary"></a>
<a id="tocSv2cbaclientportfoliocurrencysummary"></a>
<a id="tocsv2cbaclientportfoliocurrencysummary"></a>

```json
{
  "currency": "USD",
  "account_count": 0,
  "active_count": 0,
  "dormant_count": 0,
  "suspended_count": 0,
  "pending_count": 0,
  "closed_count": 0,
  "total_available_balance": 0,
  "total_interest_accrued": "2.50"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|currency|string|true|none|none|
|account_count|integer|true|none|none|
|active_count|integer|true|none|none|
|dormant_count|integer|true|none|none|
|suspended_count|integer|true|none|none|
|pending_count|integer|true|none|none|
|closed_count|integer|true|none|none|
|total_available_balance|integer(int64)|true|none|none|
|total_interest_accrued|string|true|none|none|

<h2 id="tocS_V2CBAClientPortfolioReport">V2CBAClientPortfolioReport</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaclientportfolioreport"></a>
<a id="schema_V2CBAClientPortfolioReport"></a>
<a id="tocSv2cbaclientportfolioreport"></a>
<a id="tocsv2cbaclientportfolioreport"></a>

```json
{
  "client": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "client_number": "CL-2026-000123",
    "type": "individual",
    "status": "active",
    "kyc_level": 1,
    "kyc_status": "verified",
    "kyc_data": {},
    "contact": {
      "email": "string",
      "phone": "string",
      "address": {
        "line1": "string",
        "line2": "string",
        "city": "string",
        "state": "string",
        "country": "string",
        "postal": "string"
      }
    },
    "individual_data": {
      "first_name": "string",
      "middle_name": "string",
      "last_name": "string",
      "date_of_birth": "string",
      "gender": "string",
      "national_id_type": "string",
      "national_id_number": "string",
      "nationality": "string",
      "occupation": "string",
      "employer": "string"
    },
    "corporate_data": {
      "legal_name": "string",
      "trading_name": "string",
      "registration_number": "string",
      "tax_id": "string",
      "incorporation_date": "string",
      "incorporation_country": "string",
      "industry": "string",
      "beneficial_owners": [
        {
          "name": "string",
          "ownership_pct": "string",
          "national_id": "string"
        }
      ],
      "authorized_signatories": [
        {
          "client_id": "string",
          "role": "string",
          "signing_limit": "string"
        }
      ]
    },
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  },
  "accounts": [
    {
      "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
      "account_number": "0123456789",
      "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
      "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
      "currency": "USD",
      "status": "active",
      "wallet_id": "client-CL-2026-000123-SAV-USD-001",
      "freeze_debits": false,
      "opened_at": "2019-08-24T14:15:22Z",
      "activated_at": "2019-08-24T14:15:22Z",
      "closed_at": "2019-08-24T14:15:22Z",
      "last_activity_at": "2019-08-24T14:15:22Z",
      "interest_accrued": "2.50",
      "metadata": {},
      "available_balance": 10000
    }
  ],
  "total_accounts": 0,
  "totals_by_currency": [
    {
      "currency": "USD",
      "account_count": 0,
      "active_count": 0,
      "dormant_count": 0,
      "suspended_count": 0,
      "pending_count": 0,
      "closed_count": 0,
      "total_available_balance": 0,
      "total_interest_accrued": "2.50"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|client|[V2CBAClient](#schemav2cbaclient)|true|none|none|
|accounts|[[V2CBAClientPortfolioAccount](#schemav2cbaclientportfolioaccount)]|true|none|none|
|total_accounts|integer|true|none|none|
|totals_by_currency|[[V2CBAClientPortfolioCurrencySummary](#schemav2cbaclientportfoliocurrencysummary)]|true|none|none|

<h2 id="tocS_V2CBAClientPortfolioReportResponse">V2CBAClientPortfolioReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaclientportfolioreportresponse"></a>
<a id="schema_V2CBAClientPortfolioReportResponse"></a>
<a id="tocSv2cbaclientportfolioreportresponse"></a>
<a id="tocsv2cbaclientportfolioreportresponse"></a>

```json
{
  "data": {
    "client": {
      "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
      "client_number": "CL-2026-000123",
      "type": "individual",
      "status": "active",
      "kyc_level": 1,
      "kyc_status": "verified",
      "kyc_data": {},
      "contact": {
        "email": "string",
        "phone": "string",
        "address": {
          "line1": "string",
          "line2": "string",
          "city": "string",
          "state": "string",
          "country": "string",
          "postal": "string"
        }
      },
      "individual_data": {
        "first_name": "string",
        "middle_name": "string",
        "last_name": "string",
        "date_of_birth": "string",
        "gender": "string",
        "national_id_type": "string",
        "national_id_number": "string",
        "nationality": "string",
        "occupation": "string",
        "employer": "string"
      },
      "corporate_data": {
        "legal_name": "string",
        "trading_name": "string",
        "registration_number": "string",
        "tax_id": "string",
        "incorporation_date": "string",
        "incorporation_country": "string",
        "industry": "string",
        "beneficial_owners": [
          {
            "name": "string",
            "ownership_pct": "string",
            "national_id": "string"
          }
        ],
        "authorized_signatories": [
          {
            "client_id": "string",
            "role": "string",
            "signing_limit": "string"
          }
        ]
      },
      "created_at": "2019-08-24T14:15:22Z",
      "updated_at": "2019-08-24T14:15:22Z"
    },
    "accounts": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "account_number": "0123456789",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "USD",
        "status": "active",
        "wallet_id": "client-CL-2026-000123-SAV-USD-001",
        "freeze_debits": false,
        "opened_at": "2019-08-24T14:15:22Z",
        "activated_at": "2019-08-24T14:15:22Z",
        "closed_at": "2019-08-24T14:15:22Z",
        "last_activity_at": "2019-08-24T14:15:22Z",
        "interest_accrued": "2.50",
        "metadata": {},
        "available_balance": 10000
      }
    ],
    "total_accounts": 0,
    "totals_by_currency": [
      {
        "currency": "USD",
        "account_count": 0,
        "active_count": 0,
        "dormant_count": 0,
        "suspended_count": 0,
        "pending_count": 0,
        "closed_count": 0,
        "total_available_balance": 0,
        "total_interest_accrued": "2.50"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2CBAClientPortfolioReport](#schemav2cbaclientportfolioreport)|true|none|none|

<h2 id="tocS_V2CBAStatementTransaction">V2CBAStatementTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2cbastatementtransaction"></a>
<a id="schema_V2CBAStatementTransaction"></a>
<a id="tocSv2cbastatementtransaction"></a>
<a id="tocsv2cbastatementtransaction"></a>

```json
{
  "txid": 0,
  "timestamp": "2019-08-24T14:15:22Z",
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "metadata": {
    "admin": "true"
  },
  "balance_before": 0,
  "balance_after": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|txid|integer(int64)|true|none|none|
|timestamp|string(date-time)|true|none|none|
|postings|[[V2Posting](#schemav2posting)]|true|none|none|
|metadata|[V2Metadata](#schemav2metadata)|true|none|none|
|balance_before|integer(int64)|true|none|none|
|balance_after|integer(int64)|true|none|none|

<h2 id="tocS_V2CBAAccountStatementReport">V2CBAAccountStatementReport</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaaccountstatementreport"></a>
<a id="schema_V2CBAAccountStatementReport"></a>
<a id="tocSv2cbaaccountstatementreport"></a>
<a id="tocsv2cbaaccountstatementreport"></a>

```json
{
  "account": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "account_number": "0123456789",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "USD",
    "status": "active",
    "wallet_id": "client-CL-2026-000123-SAV-USD-001",
    "freeze_debits": false,
    "opened_at": "2019-08-24T14:15:22Z",
    "activated_at": "2019-08-24T14:15:22Z",
    "closed_at": "2019-08-24T14:15:22Z",
    "last_activity_at": "2019-08-24T14:15:22Z",
    "interest_accrued": "2.50",
    "metadata": {}
  },
  "start_time": "2019-08-24T14:15:22Z",
  "end_time": "2019-08-24T14:15:22Z",
  "opening_balance": 0,
  "closing_balance": 0,
  "total_credits": 0,
  "total_debits": 0,
  "transaction_count": 0,
  "transactions": [
    {
      "txid": 0,
      "timestamp": "2019-08-24T14:15:22Z",
      "postings": [
        {
          "amount": 100,
          "asset": "COIN",
          "destination": "users:002",
          "source": "users:001"
        }
      ],
      "metadata": {
        "admin": "true"
      },
      "balance_before": 0,
      "balance_after": 0
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account|[V2CBAAccount](#schemav2cbaaccount)|true|none|none|
|start_time|string(date-time)|false|none|none|
|end_time|string(date-time)|false|none|none|
|opening_balance|integer(int64)|true|none|none|
|closing_balance|integer(int64)|true|none|none|
|total_credits|integer(int64)|true|none|none|
|total_debits|integer(int64)|true|none|none|
|transaction_count|integer|true|none|none|
|transactions|[[V2CBAStatementTransaction](#schemav2cbastatementtransaction)]|true|none|none|

<h2 id="tocS_V2CBAAccountStatementReportResponse">V2CBAAccountStatementReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2cbaaccountstatementreportresponse"></a>
<a id="schema_V2CBAAccountStatementReportResponse"></a>
<a id="tocSv2cbaaccountstatementreportresponse"></a>
<a id="tocsv2cbaaccountstatementreportresponse"></a>

```json
{
  "data": {
    "account": {
      "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
      "account_number": "0123456789",
      "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
      "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
      "currency": "USD",
      "status": "active",
      "wallet_id": "client-CL-2026-000123-SAV-USD-001",
      "freeze_debits": false,
      "opened_at": "2019-08-24T14:15:22Z",
      "activated_at": "2019-08-24T14:15:22Z",
      "closed_at": "2019-08-24T14:15:22Z",
      "last_activity_at": "2019-08-24T14:15:22Z",
      "interest_accrued": "2.50",
      "metadata": {}
    },
    "start_time": "2019-08-24T14:15:22Z",
    "end_time": "2019-08-24T14:15:22Z",
    "opening_balance": 0,
    "closing_balance": 0,
    "total_credits": 0,
    "total_debits": 0,
    "transaction_count": 0,
    "transactions": [
      {
        "txid": 0,
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "metadata": {
          "admin": "true"
        },
        "balance_before": 0,
        "balance_after": 0
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2CBAAccountStatementReport](#schemav2cbaaccountstatementreport)|true|none|none|

<h2 id="tocS_V2CBADailyTransactionAccountSummary">V2CBADailyTransactionAccountSummary</h2>
<!-- backwards compatibility -->
<a id="schemav2cbadailytransactionaccountsummary"></a>
<a id="schema_V2CBADailyTransactionAccountSummary"></a>
<a id="tocSv2cbadailytransactionaccountsummary"></a>
<a id="tocsv2cbadailytransactionaccountsummary"></a>

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "account_number": "string",
  "currency": "string",
  "transaction_count": 0,
  "debit_count": 0,
  "credit_count": 0,
  "debit_amount": 0,
  "credit_amount": 0,
  "net_amount": 0,
  "operation_counts": {
    "property1": 0,
    "property2": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account_id|string(uuid)|true|none|none|
|account_number|string|true|none|none|
|currency|string|true|none|none|
|transaction_count|integer(int64)|true|none|none|
|debit_count|integer(int64)|true|none|none|
|credit_count|integer(int64)|true|none|none|
|debit_amount|integer(int64)|true|none|none|
|credit_amount|integer(int64)|true|none|none|
|net_amount|integer(int64)|true|none|none|
|operation_counts|object|true|none|none|
|» **additionalProperties**|integer(int64)|false|none|none|

<h2 id="tocS_V2CBADailyTransactionSummaryReport">V2CBADailyTransactionSummaryReport</h2>
<!-- backwards compatibility -->
<a id="schemav2cbadailytransactionsummaryreport"></a>
<a id="schema_V2CBADailyTransactionSummaryReport"></a>
<a id="tocSv2cbadailytransactionsummaryreport"></a>
<a id="tocsv2cbadailytransactionsummaryreport"></a>

```json
{
  "report_date": "2019-08-24",
  "transaction_count": 0,
  "debit_count": 0,
  "credit_count": 0,
  "debit_amount": 0,
  "credit_amount": 0,
  "net_amount": 0,
  "operation_counts": {
    "property1": 0,
    "property2": 0
  },
  "accounts": [
    {
      "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
      "account_number": "string",
      "currency": "string",
      "transaction_count": 0,
      "debit_count": 0,
      "credit_count": 0,
      "debit_amount": 0,
      "credit_amount": 0,
      "net_amount": 0,
      "operation_counts": {
        "property1": 0,
        "property2": 0
      }
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|report_date|string(date)|true|none|none|
|transaction_count|integer(int64)|true|none|none|
|debit_count|integer(int64)|true|none|none|
|credit_count|integer(int64)|true|none|none|
|debit_amount|integer(int64)|true|none|none|
|credit_amount|integer(int64)|true|none|none|
|net_amount|integer(int64)|true|none|none|
|operation_counts|object|true|none|none|
|» **additionalProperties**|integer(int64)|false|none|none|
|accounts|[[V2CBADailyTransactionAccountSummary](#schemav2cbadailytransactionaccountsummary)]|true|none|none|

<h2 id="tocS_V2CBADailyTransactionSummaryReportResponse">V2CBADailyTransactionSummaryReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2cbadailytransactionsummaryreportresponse"></a>
<a id="schema_V2CBADailyTransactionSummaryReportResponse"></a>
<a id="tocSv2cbadailytransactionsummaryreportresponse"></a>
<a id="tocsv2cbadailytransactionsummaryreportresponse"></a>

```json
{
  "data": {
    "report_date": "2019-08-24",
    "transaction_count": 0,
    "debit_count": 0,
    "credit_count": 0,
    "debit_amount": 0,
    "credit_amount": 0,
    "net_amount": 0,
    "operation_counts": {
      "property1": 0,
      "property2": 0
    },
    "accounts": [
      {
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "account_number": "string",
        "currency": "string",
        "transaction_count": 0,
        "debit_count": 0,
        "credit_count": 0,
        "debit_amount": 0,
        "credit_amount": 0,
        "net_amount": 0,
        "operation_counts": {
          "property1": 0,
          "property2": 0
        }
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2CBADailyTransactionSummaryReport](#schemav2cbadailytransactionsummaryreport)|true|none|none|

<h2 id="tocS_V2CBAInterestFeeAccountSummary">V2CBAInterestFeeAccountSummary</h2>
<!-- backwards compatibility -->
<a id="schemav2cbainterestfeeaccountsummary"></a>
<a id="schema_V2CBAInterestFeeAccountSummary"></a>
<a id="tocSv2cbainterestfeeaccountsummary"></a>
<a id="tocsv2cbainterestfeeaccountsummary"></a>

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "account_number": "string",
  "currency": "string",
  "interest_accrued": "2.50",
  "interest_posted_count": 0,
  "interest_pending_count": 0,
  "fee_total": "10.00",
  "fee_posted_amount": "7.50",
  "fee_pending_recovery_amount": "2.50",
  "fee_posted_count": 0,
  "fee_pending_recovery_count": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account_id|string(uuid)|true|none|none|
|account_number|string|true|none|none|
|currency|string|true|none|none|
|interest_accrued|string|true|none|none|
|interest_posted_count|integer(int64)|true|none|none|
|interest_pending_count|integer(int64)|true|none|none|
|fee_total|string|true|none|none|
|fee_posted_amount|string|true|none|none|
|fee_pending_recovery_amount|string|true|none|none|
|fee_posted_count|integer(int64)|true|none|none|
|fee_pending_recovery_count|integer(int64)|true|none|none|

<h2 id="tocS_V2CBAInterestFeeTotals">V2CBAInterestFeeTotals</h2>
<!-- backwards compatibility -->
<a id="schemav2cbainterestfeetotals"></a>
<a id="schema_V2CBAInterestFeeTotals"></a>
<a id="tocSv2cbainterestfeetotals"></a>
<a id="tocsv2cbainterestfeetotals"></a>

```json
{
  "interest_accrued": "2.50",
  "interest_posted_count": 0,
  "interest_pending_count": 0,
  "fee_total": "10.00",
  "fee_posted_amount": "7.50",
  "fee_pending_recovery_amount": "2.50",
  "fee_posted_count": 0,
  "fee_pending_recovery_count": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|interest_accrued|string|true|none|none|
|interest_posted_count|integer(int64)|true|none|none|
|interest_pending_count|integer(int64)|true|none|none|
|fee_total|string|true|none|none|
|fee_posted_amount|string|true|none|none|
|fee_pending_recovery_amount|string|true|none|none|
|fee_posted_count|integer(int64)|true|none|none|
|fee_pending_recovery_count|integer(int64)|true|none|none|

<h2 id="tocS_V2CBAInterestFeeReport">V2CBAInterestFeeReport</h2>
<!-- backwards compatibility -->
<a id="schemav2cbainterestfeereport"></a>
<a id="schema_V2CBAInterestFeeReport"></a>
<a id="tocSv2cbainterestfeereport"></a>
<a id="tocsv2cbainterestfeereport"></a>

```json
{
  "accounts": [
    {
      "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
      "account_number": "string",
      "currency": "string",
      "interest_accrued": "2.50",
      "interest_posted_count": 0,
      "interest_pending_count": 0,
      "fee_total": "10.00",
      "fee_posted_amount": "7.50",
      "fee_pending_recovery_amount": "2.50",
      "fee_posted_count": 0,
      "fee_pending_recovery_count": 0
    }
  ],
  "totals": {
    "interest_accrued": "2.50",
    "interest_posted_count": 0,
    "interest_pending_count": 0,
    "fee_total": "10.00",
    "fee_posted_amount": "7.50",
    "fee_pending_recovery_amount": "2.50",
    "fee_posted_count": 0,
    "fee_pending_recovery_count": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|accounts|[[V2CBAInterestFeeAccountSummary](#schemav2cbainterestfeeaccountsummary)]|true|none|none|
|totals|[V2CBAInterestFeeTotals](#schemav2cbainterestfeetotals)|true|none|none|

<h2 id="tocS_V2CBAInterestFeeReportResponse">V2CBAInterestFeeReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2cbainterestfeereportresponse"></a>
<a id="schema_V2CBAInterestFeeReportResponse"></a>
<a id="tocSv2cbainterestfeereportresponse"></a>
<a id="tocsv2cbainterestfeereportresponse"></a>

```json
{
  "data": {
    "accounts": [
      {
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "account_number": "string",
        "currency": "string",
        "interest_accrued": "2.50",
        "interest_posted_count": 0,
        "interest_pending_count": 0,
        "fee_total": "10.00",
        "fee_posted_amount": "7.50",
        "fee_pending_recovery_amount": "2.50",
        "fee_posted_count": 0,
        "fee_pending_recovery_count": 0
      }
    ],
    "totals": {
      "interest_accrued": "2.50",
      "interest_posted_count": 0,
      "interest_pending_count": 0,
      "fee_total": "10.00",
      "fee_posted_amount": "7.50",
      "fee_pending_recovery_amount": "2.50",
      "fee_posted_count": 0,
      "fee_pending_recovery_count": 0
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2CBAInterestFeeReport](#schemav2cbainterestfeereport)|true|none|none|

<h2 id="tocS_V2FinanceTrialBalanceLine">V2FinanceTrialBalanceLine</h2>
<!-- backwards compatibility -->
<a id="schemav2financetrialbalanceline"></a>
<a id="schema_V2FinanceTrialBalanceLine"></a>
<a id="tocSv2financetrialbalanceline"></a>
<a id="tocsv2financetrialbalanceline"></a>

```json
{
  "ledger": "string",
  "account": "string",
  "balance": 0,
  "currency": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ledger|string|true|none|none|
|account|string|true|none|none|
|balance|integer(int64)|true|none|none|
|currency|string|true|none|none|

<h2 id="tocS_V2FinanceTrialBalanceReport">V2FinanceTrialBalanceReport</h2>
<!-- backwards compatibility -->
<a id="schemav2financetrialbalancereport"></a>
<a id="schema_V2FinanceTrialBalanceReport"></a>
<a id="tocSv2financetrialbalancereport"></a>
<a id="tocsv2financetrialbalancereport"></a>

```json
{
  "currency": "string",
  "as_of": "2019-08-24T14:15:22Z",
  "lines": [
    {
      "ledger": "string",
      "account": "string",
      "balance": 0,
      "currency": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|currency|string|true|none|none|
|as_of|string(date-time)|false|none|none|
|lines|[[V2FinanceTrialBalanceLine](#schemav2financetrialbalanceline)]|true|none|none|

<h2 id="tocS_V2FinanceTrialBalanceReportResponse">V2FinanceTrialBalanceReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2financetrialbalancereportresponse"></a>
<a id="schema_V2FinanceTrialBalanceReportResponse"></a>
<a id="tocSv2financetrialbalancereportresponse"></a>
<a id="tocsv2financetrialbalancereportresponse"></a>

```json
{
  "data": {
    "currency": "string",
    "as_of": "2019-08-24T14:15:22Z",
    "lines": [
      {
        "ledger": "string",
        "account": "string",
        "balance": 0,
        "currency": "string"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2FinanceTrialBalanceReport](#schemav2financetrialbalancereport)|true|none|none|

<h2 id="tocS_V2GLAccountType">V2GLAccountType</h2>
<!-- backwards compatibility -->
<a id="schemav2glaccounttype"></a>
<a id="schema_V2GLAccountType"></a>
<a id="tocSv2glaccounttype"></a>
<a id="tocsv2glaccounttype"></a>

```json
"asset"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|asset|
|*anonymous*|liability|
|*anonymous*|equity|
|*anonymous*|income|
|*anonymous*|expense|

<h2 id="tocS_V2GLAccountStatus">V2GLAccountStatus</h2>
<!-- backwards compatibility -->
<a id="schemav2glaccountstatus"></a>
<a id="schema_V2GLAccountStatus"></a>
<a id="tocSv2glaccountstatus"></a>
<a id="tocsv2glaccountstatus"></a>

```json
"active"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|active|
|*anonymous*|closed|

<h2 id="tocS_V2GLSide">V2GLSide</h2>
<!-- backwards compatibility -->
<a id="schemav2glside"></a>
<a id="schema_V2GLSide"></a>
<a id="tocSv2glside"></a>
<a id="tocsv2glside"></a>

```json
"debit"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|debit|
|*anonymous*|credit|

<h2 id="tocS_V2CreateGLAccountRequest">V2CreateGLAccountRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createglaccountrequest"></a>
<a id="schema_V2CreateGLAccountRequest"></a>
<a id="tocSv2createglaccountrequest"></a>
<a id="tocsv2createglaccountrequest"></a>

```json
{
  "code": "1001",
  "name": "string",
  "type": "asset",
  "normal_side": "debit",
  "currency": "NGN",
  "parent_code": "string",
  "ledger": "ledgertrack",
  "address": "gl:assets:cash",
  "metadata": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|code|string|true|none|Unique across the registry.|
|name|string|true|none|none|
|type|[V2GLAccountType](#schemav2glaccounttype)|true|none|none|
|normal_side|[V2GLSide](#schemav2glside)|false|none|Defaults from the type (asset and expense are debit, the others credit).|
|currency|string|true|none|Must be enabled in the currency registry.|
|parent_code|string|false|none|Must exist, with the same currency and type, and be active.|
|ledger|string|true|none|none|
|address|string|true|none|Ledger address, must start with `gl:`; unique per ledger.|
|metadata|object|false|none|none|

<h2 id="tocS_V2PatchGLAccountRequest">V2PatchGLAccountRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2patchglaccountrequest"></a>
<a id="schema_V2PatchGLAccountRequest"></a>
<a id="tocSv2patchglaccountrequest"></a>
<a id="tocsv2patchglaccountrequest"></a>

```json
{
  "name": "string",
  "status": "active",
  "metadata": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|false|none|none|
|status|[V2GLAccountStatus](#schemav2glaccountstatus)|false|none|none|
|metadata|object|false|none|none|

<h2 id="tocS_V2GLAccount">V2GLAccount</h2>
<!-- backwards compatibility -->
<a id="schemav2glaccount"></a>
<a id="schema_V2GLAccount"></a>
<a id="tocSv2glaccount"></a>
<a id="tocsv2glaccount"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "code": "string",
  "name": "string",
  "type": "asset",
  "normal_side": "debit",
  "currency": "string",
  "parent_code": "string",
  "ledger": "string",
  "address": "string",
  "status": "active",
  "metadata": {},
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|code|string|true|none|none|
|name|string|true|none|none|
|type|[V2GLAccountType](#schemav2glaccounttype)|true|none|none|
|normal_side|[V2GLSide](#schemav2glside)|true|none|none|
|currency|string|true|none|none|
|parent_code|string¦null|false|none|none|
|ledger|string|true|none|none|
|address|string|true|none|none|
|status|[V2GLAccountStatus](#schemav2glaccountstatus)|true|none|none|
|metadata|object|false|none|none|
|created_at|string(date-time)|false|none|none|
|updated_at|string(date-time)|false|none|none|

<h2 id="tocS_V2FeeTier">V2FeeTier</h2>
<!-- backwards compatibility -->
<a id="schemav2feetier"></a>
<a id="schema_V2FeeTier"></a>
<a id="tocSv2feetier"></a>
<a id="tocsv2feetier"></a>

```json
{
  "from": "0",
  "to": "string",
  "type": "flat",
  "value": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|from|string|true|none|none|
|to|string|false|none|Exclusive upper bound. Omitted on the last tier.|
|type|string|true|none|none|
|value|string|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|type|flat|
|type|percentage|

<h2 id="tocS_V2TransactionFee">V2TransactionFee</h2>
<!-- backwards compatibility -->
<a id="schemav2transactionfee"></a>
<a id="schema_V2TransactionFee"></a>
<a id="tocSv2transactionfee"></a>
<a id="tocsv2transactionfee"></a>

```json
{
  "event": "debit",
  "type": "flat",
  "value": "string",
  "tiers": [
    {
      "from": "0",
      "to": "string",
      "type": "flat",
      "value": "string"
    }
  ],
  "min": "string",
  "max": "string",
  "currency": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|event|string|true|none|none|
|type|string|true|none|none|
|value|string|false|none|Amount for `flat`, percent for `percentage`. Not used for `tiered`.|
|tiers|[[V2FeeTier](#schemav2feetier)]|false|none|Bands for `tiered`. They start at 0, touch each other and the last one is open ended.|
|min|string|false|none|none|
|max|string|false|none|none|
|currency|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|event|debit|
|event|credit|
|event|transfer|
|type|flat|
|type|percentage|
|type|tiered|

<h2 id="tocS_V2CreateFeeOverrideRequest">V2CreateFeeOverrideRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createfeeoverriderequest"></a>
<a id="schema_V2CreateFeeOverrideRequest"></a>
<a id="tocSv2createfeeoverriderequest"></a>
<a id="tocsv2createfeeoverriderequest"></a>

```json
{
  "scope": "client",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "NGN",
  "event": "debit",
  "rules": [
    {
      "event": "debit",
      "type": "flat",
      "value": "string",
      "tiers": [
        {
          "from": "0",
          "to": "string",
          "type": "flat",
          "value": "string"
        }
      ],
      "min": "string",
      "max": "string",
      "currency": "string"
    }
  ],
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "metadata": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|scope|string|true|none|none|
|client_id|string(uuid)|false|none|none|
|account_id|string(uuid)|false|none|none|
|product_id|string(uuid)|false|none|Client scope only. Limits the override to accounts of this product.|
|currency|string|true|none|none|
|event|string|true|none|none|
|rules|[[V2TransactionFee](#schemav2transactionfee)]|true|none|none|
|effective_from|string(date-time)|false|none|none|
|effective_to|string(date-time)|false|none|none|
|metadata|object|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|scope|client|
|scope|account|
|event|debit|
|event|credit|
|event|transfer|

<h2 id="tocS_V2FeeOverride">V2FeeOverride</h2>
<!-- backwards compatibility -->
<a id="schemav2feeoverride"></a>
<a id="schema_V2FeeOverride"></a>
<a id="tocSv2feeoverride"></a>
<a id="tocsv2feeoverride"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "scope": "string",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "string",
  "event": "string",
  "rules": [
    {
      "event": "debit",
      "type": "flat",
      "value": "string",
      "tiers": [
        {
          "from": "0",
          "to": "string",
          "type": "flat",
          "value": "string"
        }
      ],
      "min": "string",
      "max": "string",
      "currency": "string"
    }
  ],
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "status": "active",
  "metadata": {},
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|scope|string|true|none|none|
|client_id|string(uuid)|false|none|none|
|account_id|string(uuid)|false|none|none|
|product_id|string(uuid)|false|none|none|
|currency|string|true|none|none|
|event|string|true|none|none|
|rules|[[V2TransactionFee](#schemav2transactionfee)]|true|none|none|
|effective_from|string(date-time)|true|none|none|
|effective_to|string(date-time)|false|none|none|
|status|string|true|none|none|
|metadata|object|false|none|none|
|created_at|string(date-time)|true|none|none|
|updated_at|string(date-time)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|status|active|
|status|revoked|

<h2 id="tocS_V2FeeOverrideResponse">V2FeeOverrideResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2feeoverrideresponse"></a>
<a id="schema_V2FeeOverrideResponse"></a>
<a id="tocSv2feeoverrideresponse"></a>
<a id="tocsv2feeoverrideresponse"></a>

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "string",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "event": "string",
    "rules": [
      {
        "event": "debit",
        "type": "flat",
        "value": "string",
        "tiers": [
          {
            "from": "0",
            "to": "string",
            "type": "flat",
            "value": "string"
          }
        ],
        "min": "string",
        "max": "string",
        "currency": "string"
      }
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2FeeOverride](#schemav2feeoverride)|true|none|none|

<h2 id="tocS_V2FeeOverrideListResponse">V2FeeOverrideListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2feeoverridelistresponse"></a>
<a id="schema_V2FeeOverrideListResponse"></a>
<a id="tocSv2feeoverridelistresponse"></a>
<a id="tocsv2feeoverridelistresponse"></a>

```json
{
  "data": {
    "overrides": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "scope": "string",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "string",
        "event": "string",
        "rules": [
          {
            "event": "debit",
            "type": "flat",
            "value": "string",
            "tiers": [
              {
                "from": "0",
                "to": "string",
                "type": "flat",
                "value": "string"
              }
            ],
            "min": "string",
            "max": "string",
            "currency": "string"
          }
        ],
        "effective_from": "2019-08-24T14:15:22Z",
        "effective_to": "2019-08-24T14:15:22Z",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» overrides|[[V2FeeOverride](#schemav2feeoverride)]|true|none|none|

<h2 id="tocS_V2CreateTaxRuleRequest">V2CreateTaxRuleRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createtaxrulerequest"></a>
<a id="schema_V2CreateTaxRuleRequest"></a>
<a id="tocSv2createtaxrulerequest"></a>
<a id="tocsv2createtaxrulerequest"></a>

```json
{
  "country": "NG",
  "code": "VAT",
  "name": "Value added tax",
  "event": "debit",
  "base": "fee",
  "rate": "7.5",
  "min": "string",
  "max": "string",
  "payable_accounts": {
    "NGN": "202100.NGN"
  },
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "metadata": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|country|string|true|none|ISO 3166-1 alpha-2.|
|code|string|true|none|none|
|name|string|true|none|none|
|event|string|true|none|none|
|base|string|true|none|none|
|rate|string|true|none|Percent, so "7.5" is 7.5%.|
|min|string|false|none|none|
|max|string|false|none|none|
|payable_accounts|object|true|none|Liability GL account code per currency.|
|» **additionalProperties**|string|false|none|none|
|effective_from|string(date-time)|false|none|none|
|effective_to|string(date-time)|false|none|none|
|metadata|object|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|event|debit|
|event|credit|
|event|transfer|
|base|fee|
|base|amount|

<h2 id="tocS_V2TaxRule">V2TaxRule</h2>
<!-- backwards compatibility -->
<a id="schemav2taxrule"></a>
<a id="schema_V2TaxRule"></a>
<a id="tocSv2taxrule"></a>
<a id="tocsv2taxrule"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "country": "string",
  "code": "string",
  "name": "string",
  "event": "string",
  "base": "string",
  "rate": "string",
  "min": "string",
  "max": "string",
  "payable_accounts": {
    "property1": "string",
    "property2": "string"
  },
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "status": "active",
  "metadata": {},
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|country|string|true|none|none|
|code|string|true|none|none|
|name|string|true|none|none|
|event|string|true|none|none|
|base|string|true|none|none|
|rate|string|true|none|none|
|min|string|false|none|none|
|max|string|false|none|none|
|payable_accounts|object|true|none|none|
|» **additionalProperties**|string|false|none|none|
|effective_from|string(date-time)|true|none|none|
|effective_to|string(date-time)|false|none|none|
|status|string|true|none|none|
|metadata|object|false|none|none|
|created_at|string(date-time)|true|none|none|
|updated_at|string(date-time)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|status|active|
|status|revoked|

<h2 id="tocS_V2TaxRuleResponse">V2TaxRuleResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2taxruleresponse"></a>
<a id="schema_V2TaxRuleResponse"></a>
<a id="tocSv2taxruleresponse"></a>
<a id="tocsv2taxruleresponse"></a>

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "country": "string",
    "code": "string",
    "name": "string",
    "event": "string",
    "base": "string",
    "rate": "string",
    "min": "string",
    "max": "string",
    "payable_accounts": {
      "property1": "string",
      "property2": "string"
    },
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2TaxRule](#schemav2taxrule)|true|none|none|

<h2 id="tocS_V2TaxRuleListResponse">V2TaxRuleListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2taxrulelistresponse"></a>
<a id="schema_V2TaxRuleListResponse"></a>
<a id="tocSv2taxrulelistresponse"></a>
<a id="tocsv2taxrulelistresponse"></a>

```json
{
  "data": {
    "rules": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "country": "string",
        "code": "string",
        "name": "string",
        "event": "string",
        "base": "string",
        "rate": "string",
        "min": "string",
        "max": "string",
        "payable_accounts": {
          "property1": "string",
          "property2": "string"
        },
        "effective_from": "2019-08-24T14:15:22Z",
        "effective_to": "2019-08-24T14:15:22Z",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» rules|[[V2TaxRule](#schemav2taxrule)]|true|none|none|

<h2 id="tocS_V2FeeQuoteRequest">V2FeeQuoteRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2feequoterequest"></a>
<a id="schema_V2FeeQuoteRequest"></a>
<a id="tocSv2feequoterequest"></a>
<a id="tocsv2feequoterequest"></a>

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "event": "debit",
  "amount_minor": 0,
  "at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account_id|string(uuid)|true|none|none|
|event|string|true|none|none|
|amount_minor|integer(int64)|true|none|Transaction amount in minor units.|
|at|string(date-time)|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|event|debit|
|event|credit|
|event|transfer|

<h2 id="tocS_V2FeeQuoteFeeLine">V2FeeQuoteFeeLine</h2>
<!-- backwards compatibility -->
<a id="schemav2feequotefeeline"></a>
<a id="schema_V2FeeQuoteFeeLine"></a>
<a id="tocSv2feequotefeeline"></a>
<a id="tocsv2feequotefeeline"></a>

```json
{
  "type": "string",
  "amount": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|type|string|true|none|none|
|amount|string|true|none|none|

<h2 id="tocS_V2FeeQuoteTaxLine">V2FeeQuoteTaxLine</h2>
<!-- backwards compatibility -->
<a id="schemav2feequotetaxline"></a>
<a id="schema_V2FeeQuoteTaxLine"></a>
<a id="tocSv2feequotetaxline"></a>
<a id="tocsv2feequotetaxline"></a>

```json
{
  "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
  "code": "string",
  "base": "string",
  "rate": "string",
  "amount": "string",
  "payable_account_code": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|rule_id|string(uuid)|true|none|none|
|code|string|true|none|none|
|base|string|true|none|none|
|rate|string|true|none|none|
|amount|string|true|none|none|
|payable_account_code|string|true|none|none|

<h2 id="tocS_V2FeeQuote">V2FeeQuote</h2>
<!-- backwards compatibility -->
<a id="schemav2feequote"></a>
<a id="schema_V2FeeQuote"></a>
<a id="tocSv2feequote"></a>
<a id="tocsv2feequote"></a>

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "event": "string",
  "currency": "string",
  "amount": "string",
  "fee": "string",
  "tax": "string",
  "gross": "string",
  "total": "string",
  "amount_minor": 0,
  "fee_minor": 0,
  "tax_minor": 0,
  "total_minor": 0,
  "fee_source": "account",
  "fee_override_id": "77f4042c-f990-41c0-9a3f-ccbfd4e7b296",
  "fee_lines": [
    {
      "type": "string",
      "amount": "string"
    }
  ],
  "tax_lines": [
    {
      "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
      "code": "string",
      "base": "string",
      "rate": "string",
      "amount": "string",
      "payable_account_code": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account_id|string(uuid)|true|none|none|
|event|string|true|none|none|
|currency|string|true|none|none|
|amount|string|true|none|none|
|fee|string|true|none|none|
|tax|string|true|none|none|
|gross|string|true|none|Fee plus tax.|
|total|string|true|none|Amount plus fee plus tax, what leaves the customer's wallet.|
|amount_minor|integer(int64)|true|none|none|
|fee_minor|integer(int64)|true|none|none|
|tax_minor|integer(int64)|true|none|none|
|total_minor|integer(int64)|true|none|none|
|fee_source|string|true|none|none|
|fee_override_id|string(uuid)|false|none|none|
|fee_lines|[[V2FeeQuoteFeeLine](#schemav2feequotefeeline)]|true|none|none|
|tax_lines|[[V2FeeQuoteTaxLine](#schemav2feequotetaxline)]|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|fee_source|account|
|fee_source|client|
|fee_source|product|
|fee_source|none|

<h2 id="tocS_V2BusinessCalendarRequest">V2BusinessCalendarRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2businesscalendarrequest"></a>
<a id="schema_V2BusinessCalendarRequest"></a>
<a id="tocSv2businesscalendarrequest"></a>
<a id="tocsv2businesscalendarrequest"></a>

```json
{
  "code": "string",
  "name": "string",
  "timezone": "Africa/Lagos",
  "working_days": [
    1
  ],
  "open_time": "08:00",
  "close_time": "17:00",
  "holidays": [
    "2019-08-24"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|code|string|false|none|Required on create. Cannot change on update.|
|name|string|true|none|none|
|timezone|string|true|none|none|
|working_days|[integer]|true|none|none|
|open_time|string|true|none|none|
|close_time|string|true|none|none|
|holidays|[string]|false|none|none|

<h2 id="tocS_V2BusinessCalendar">V2BusinessCalendar</h2>
<!-- backwards compatibility -->
<a id="schemav2businesscalendar"></a>
<a id="schema_V2BusinessCalendar"></a>
<a id="tocSv2businesscalendar"></a>
<a id="tocsv2businesscalendar"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "code": "string",
  "name": "string",
  "timezone": "string",
  "working_days": [
    0
  ],
  "open_time": "string",
  "close_time": "string",
  "holidays": [
    "2019-08-24"
  ],
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|code|string|true|none|none|
|name|string|true|none|none|
|timezone|string|true|none|none|
|working_days|[integer]|true|none|none|
|open_time|string|true|none|none|
|close_time|string|true|none|none|
|holidays|[string]|true|none|none|
|created_at|string(date-time)|true|none|none|
|updated_at|string(date-time)|true|none|none|

<h2 id="tocS_V2BusinessCalendarResponse">V2BusinessCalendarResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2businesscalendarresponse"></a>
<a id="schema_V2BusinessCalendarResponse"></a>
<a id="tocSv2businesscalendarresponse"></a>
<a id="tocsv2businesscalendarresponse"></a>

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "timezone": "string",
    "working_days": [
      0
    ],
    "open_time": "string",
    "close_time": "string",
    "holidays": [
      "2019-08-24"
    ],
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2BusinessCalendar](#schemav2businesscalendar)|true|none|none|

<h2 id="tocS_V2BusinessCalendarListResponse">V2BusinessCalendarListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2businesscalendarlistresponse"></a>
<a id="schema_V2BusinessCalendarListResponse"></a>
<a id="tocSv2businesscalendarlistresponse"></a>
<a id="tocsv2businesscalendarlistresponse"></a>

```json
{
  "data": {
    "calendars": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "code": "string",
        "name": "string",
        "timezone": "string",
        "working_days": [
          0
        ],
        "open_time": "string",
        "close_time": "string",
        "holidays": [
          "2019-08-24"
        ],
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» calendars|[[V2BusinessCalendar](#schemav2businesscalendar)]|true|none|none|

<h2 id="tocS_V2CreateReserveRuleRequest">V2CreateReserveRuleRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createreserverulerequest"></a>
<a id="schema_V2CreateReserveRuleRequest"></a>
<a id="tocSv2createreserverulerequest"></a>
<a id="tocsv2createreserverulerequest"></a>

```json
{
  "scope": "client",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "string",
  "percentage": "string",
  "hold_hours": 1,
  "hold_mode": "calendar",
  "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
  "credit_types": [
    "string"
  ],
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "metadata": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|scope|string|true|none|none|
|client_id|string(uuid)|false|none|none|
|account_id|string(uuid)|false|none|none|
|product_id|string(uuid)|false|none|none|
|currency|string|false|none|Required for a client rule. An account rule takes the account currency.|
|percentage|string|true|none|Share of each credit that is held, as a decimal string. "10" is 10%.|
|hold_hours|integer|true|none|none|
|hold_mode|string|false|none|calendar counts hold_hours as clock hours. business counts only the hours inside the business calendar.|
|calendar_id|string(uuid)|false|none|Required for hold_mode business.|
|credit_types|[string]|false|none|Limits the rule to these credit types (lowercase, digits and underscores). Empty covers every credit.|
|effective_from|string(date-time)|false|none|none|
|effective_to|string(date-time)|false|none|none|
|metadata|object|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|scope|client|
|scope|account|
|hold_mode|calendar|
|hold_mode|business|

<h2 id="tocS_V2ReserveRule">V2ReserveRule</h2>
<!-- backwards compatibility -->
<a id="schemav2reserverule"></a>
<a id="schema_V2ReserveRule"></a>
<a id="tocSv2reserverule"></a>
<a id="tocsv2reserverule"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "scope": "client",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
  "currency": "string",
  "percentage": "string",
  "hold_hours": 0,
  "hold_mode": "calendar",
  "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
  "credit_types": [
    "string"
  ],
  "effective_from": "2019-08-24T14:15:22Z",
  "effective_to": "2019-08-24T14:15:22Z",
  "status": "active",
  "metadata": {},
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|scope|string|true|none|none|
|client_id|string(uuid)|false|none|none|
|account_id|string(uuid)|false|none|none|
|product_id|string(uuid)|false|none|none|
|currency|string|true|none|none|
|percentage|string|true|none|none|
|hold_hours|integer|true|none|none|
|hold_mode|string|false|none|none|
|calendar_id|string(uuid)|false|none|none|
|credit_types|[string]|false|none|none|
|effective_from|string(date-time)|true|none|none|
|effective_to|string(date-time)|false|none|none|
|status|string|true|none|none|
|metadata|object|false|none|none|
|created_at|string(date-time)|true|none|none|
|updated_at|string(date-time)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|scope|client|
|scope|account|
|hold_mode|calendar|
|hold_mode|business|
|status|active|
|status|revoked|

<h2 id="tocS_V2ReserveRuleResponse">V2ReserveRuleResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2reserveruleresponse"></a>
<a id="schema_V2ReserveRuleResponse"></a>
<a id="tocSv2reserveruleresponse"></a>
<a id="tocsv2reserveruleresponse"></a>

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "scope": "client",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
    "currency": "string",
    "percentage": "string",
    "hold_hours": 0,
    "hold_mode": "calendar",
    "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
    "credit_types": [
      "string"
    ],
    "effective_from": "2019-08-24T14:15:22Z",
    "effective_to": "2019-08-24T14:15:22Z",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2ReserveRule](#schemav2reserverule)|true|none|none|

<h2 id="tocS_V2ReserveRuleListResponse">V2ReserveRuleListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2reserverulelistresponse"></a>
<a id="schema_V2ReserveRuleListResponse"></a>
<a id="tocSv2reserverulelistresponse"></a>
<a id="tocsv2reserverulelistresponse"></a>

```json
{
  "data": {
    "rules": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "scope": "client",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "product_id": "0d012afa-f885-4e65-aeca-37e27701e2d1",
        "currency": "string",
        "percentage": "string",
        "hold_hours": 0,
        "hold_mode": "calendar",
        "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
        "credit_types": [
          "string"
        ],
        "effective_from": "2019-08-24T14:15:22Z",
        "effective_to": "2019-08-24T14:15:22Z",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» rules|[[V2ReserveRule](#schemav2reserverule)]|true|none|none|

<h2 id="tocS_V2DebitWalletRequest">V2DebitWalletRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2debitwalletrequest"></a>
<a id="schema_V2DebitWalletRequest"></a>
<a id="tocSv2debitwalletrequest"></a>
<a id="tocsv2debitwalletrequest"></a>

```json
{
  "amount": 0,
  "reference": "string",
  "metadata": {
    "property1": "string",
    "property2": "string"
  },
  "channelID": "string",
  "channelAmount": 0,
  "draw_from_reserve": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|amount|number|true|none|Amount in minor units.|
|reference|string|true|none|none|
|metadata|object|false|none|none|
|» **additionalProperties**|string|false|none|none|
|channelID|string|false|none|none|
|channelAmount|number|false|none|none|
|draw_from_reserve|boolean|false|none|Spend the wallet's held reserve shares first (chargebacks). Not allowed with a channel.|

<h2 id="tocS_V2CreditWalletRequest">V2CreditWalletRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2creditwalletrequest"></a>
<a id="schema_V2CreditWalletRequest"></a>
<a id="tocSv2creditwalletrequest"></a>
<a id="tocsv2creditwalletrequest"></a>

```json
{
  "amount": 0,
  "reference": "string",
  "credit_type": "string",
  "metadata": {
    "property1": "string",
    "property2": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|amount|number|true|none|Amount in minor units.|
|reference|string|true|none|none|
|credit_type|string|false|none|Kind of credit (for example `card`), matched against the `credit_types` of reserve rules.|
|metadata|object|false|none|none|
|» **additionalProperties**|string|false|none|none|

<h2 id="tocS_V2ReserveHold">V2ReserveHold</h2>
<!-- backwards compatibility -->
<a id="schemav2reservehold"></a>
<a id="schema_V2ReserveHold"></a>
<a id="tocSv2reservehold"></a>
<a id="tocsv2reservehold"></a>

```json
{
  "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
  "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
  "ledger": "string",
  "wallet_id": "string",
  "currency": "string",
  "credit_reference": "string",
  "credit_tx_id": 0,
  "credit_type": "string",
  "credit_amount_minor": 0,
  "amount_minor": 0,
  "release_at": "2019-08-24T14:15:22Z",
  "status": "pending",
  "released_at": "2019-08-24T14:15:22Z",
  "release_reason": "sla",
  "release_reference": "string",
  "attempts": 0,
  "last_error": "string",
  "created_at": "2019-08-24T14:15:22Z",
  "updated_at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|string(uuid)|true|none|none|
|rule_id|string(uuid)|false|none|none|
|account_id|string(uuid)|true|none|none|
|client_id|string(uuid)|true|none|none|
|ledger|string|true|none|none|
|wallet_id|string|true|none|none|
|currency|string|true|none|none|
|credit_reference|string|true|none|none|
|credit_tx_id|integer(int64)|false|none|none|
|credit_type|string|false|none|none|
|credit_amount_minor|integer(int64)|true|none|none|
|amount_minor|integer(int64)|true|none|The held share, in minor units.|
|release_at|string(date-time)|true|none|none|
|status|string|true|none|none|
|released_at|string(date-time)|false|none|none|
|release_reason|string|false|none|none|
|release_reference|string|false|none|none|
|attempts|integer|true|none|none|
|last_error|string|false|none|none|
|created_at|string(date-time)|true|none|none|
|updated_at|string(date-time)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|status|pending|
|status|held|
|status|released|
|status|voided|
|status|needs_review|
|release_reason|sla|
|release_reason|manual|
|release_reason|drawn|

<h2 id="tocS_V2ReserveHoldResponse">V2ReserveHoldResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2reserveholdresponse"></a>
<a id="schema_V2ReserveHoldResponse"></a>
<a id="tocSv2reserveholdresponse"></a>
<a id="tocsv2reserveholdresponse"></a>

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
    "ledger": "string",
    "wallet_id": "string",
    "currency": "string",
    "credit_reference": "string",
    "credit_tx_id": 0,
    "credit_type": "string",
    "credit_amount_minor": 0,
    "amount_minor": 0,
    "release_at": "2019-08-24T14:15:22Z",
    "status": "pending",
    "released_at": "2019-08-24T14:15:22Z",
    "release_reason": "sla",
    "release_reference": "string",
    "attempts": 0,
    "last_error": "string",
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2ReserveHold](#schemav2reservehold)|true|none|none|

<h2 id="tocS_V2ReserveHoldListResponse">V2ReserveHoldListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2reserveholdlistresponse"></a>
<a id="schema_V2ReserveHoldListResponse"></a>
<a id="tocSv2reserveholdlistresponse"></a>
<a id="tocsv2reserveholdlistresponse"></a>

```json
{
  "data": {
    "holds": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
        "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
        "client_id": "5b3fa7ba-57d3-4017-a65b-d57dcd2db643",
        "ledger": "string",
        "wallet_id": "string",
        "currency": "string",
        "credit_reference": "string",
        "credit_tx_id": 0,
        "credit_type": "string",
        "credit_amount_minor": 0,
        "amount_minor": 0,
        "release_at": "2019-08-24T14:15:22Z",
        "status": "pending",
        "released_at": "2019-08-24T14:15:22Z",
        "release_reason": "sla",
        "release_reference": "string",
        "attempts": 0,
        "last_error": "string",
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» holds|[[V2ReserveHold](#schemav2reservehold)]|true|none|none|

<h2 id="tocS_V2ReservePreviewRequest">V2ReservePreviewRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2reservepreviewrequest"></a>
<a id="schema_V2ReservePreviewRequest"></a>
<a id="tocSv2reservepreviewrequest"></a>
<a id="tocsv2reservepreviewrequest"></a>

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "amount_minor": 0,
  "credit_type": "string",
  "at": "2019-08-24T14:15:22Z"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account_id|string(uuid)|true|none|none|
|amount_minor|integer(int64)|true|none|none|
|credit_type|string|false|none|Matched against rules that list credit types. Omit to match only rules that cover every credit.|
|at|string(date-time)|false|none|none|

<h2 id="tocS_V2ReservePreview">V2ReservePreview</h2>
<!-- backwards compatibility -->
<a id="schemav2reservepreview"></a>
<a id="schema_V2ReservePreview"></a>
<a id="tocSv2reservepreview"></a>
<a id="tocsv2reservepreview"></a>

```json
{
  "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
  "currency": "string",
  "amount_minor": 0,
  "reserve_minor": 0,
  "available_minor": 0,
  "percentage": "string",
  "hold_hours": 0,
  "hold_mode": "calendar",
  "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
  "credit_type": "string",
  "release_at": "2019-08-24T14:15:22Z",
  "rule_source": "account",
  "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|account_id|string(uuid)|true|none|none|
|currency|string|true|none|none|
|amount_minor|integer(int64)|true|none|none|
|reserve_minor|integer(int64)|true|none|none|
|available_minor|integer(int64)|true|none|none|
|percentage|string|false|none|none|
|hold_hours|integer|false|none|none|
|hold_mode|string|false|none|none|
|calendar_id|string(uuid)|false|none|none|
|credit_type|string|false|none|none|
|release_at|string(date-time)|false|none|none|
|rule_source|string|true|none|none|
|rule_id|string(uuid)|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|hold_mode|calendar|
|hold_mode|business|
|rule_source|account|
|rule_source|client|
|rule_source|none|

<h2 id="tocS_V2ReservePreviewResponse">V2ReservePreviewResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2reservepreviewresponse"></a>
<a id="schema_V2ReservePreviewResponse"></a>
<a id="tocSv2reservepreviewresponse"></a>
<a id="tocsv2reservepreviewresponse"></a>

```json
{
  "data": {
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "currency": "string",
    "amount_minor": 0,
    "reserve_minor": 0,
    "available_minor": 0,
    "percentage": "string",
    "hold_hours": 0,
    "hold_mode": "calendar",
    "calendar_id": "c828ce68-4c5a-4491-b76a-af3d888d3f97",
    "credit_type": "string",
    "release_at": "2019-08-24T14:15:22Z",
    "rule_source": "account",
    "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2ReservePreview](#schemav2reservepreview)|true|none|none|

<h2 id="tocS_V2FeeQuoteResponse">V2FeeQuoteResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2feequoteresponse"></a>
<a id="schema_V2FeeQuoteResponse"></a>
<a id="tocSv2feequoteresponse"></a>
<a id="tocsv2feequoteresponse"></a>

```json
{
  "data": {
    "account_id": "449e7a5c-69d3-4b8a-aaaf-5c9b713ebc65",
    "event": "string",
    "currency": "string",
    "amount": "string",
    "fee": "string",
    "tax": "string",
    "gross": "string",
    "total": "string",
    "amount_minor": 0,
    "fee_minor": 0,
    "tax_minor": 0,
    "total_minor": 0,
    "fee_source": "account",
    "fee_override_id": "77f4042c-f990-41c0-9a3f-ccbfd4e7b296",
    "fee_lines": [
      {
        "type": "string",
        "amount": "string"
      }
    ],
    "tax_lines": [
      {
        "rule_id": "728c1541-d6d1-4290-9a53-cdf01dd32d60",
        "code": "string",
        "base": "string",
        "rate": "string",
        "amount": "string",
        "payable_account_code": "string"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2FeeQuote](#schemav2feequote)|true|none|none|

<h2 id="tocS_V2GLAccountResponse">V2GLAccountResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2glaccountresponse"></a>
<a id="schema_V2GLAccountResponse"></a>
<a id="tocSv2glaccountresponse"></a>
<a id="tocsv2glaccountresponse"></a>

```json
{
  "data": {
    "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
    "code": "string",
    "name": "string",
    "type": "asset",
    "normal_side": "debit",
    "currency": "string",
    "parent_code": "string",
    "ledger": "string",
    "address": "string",
    "status": "active",
    "metadata": {},
    "created_at": "2019-08-24T14:15:22Z",
    "updated_at": "2019-08-24T14:15:22Z"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2GLAccount](#schemav2glaccount)|true|none|none|

<h2 id="tocS_V2GLAccountListResponse">V2GLAccountListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2glaccountlistresponse"></a>
<a id="schema_V2GLAccountListResponse"></a>
<a id="tocSv2glaccountlistresponse"></a>
<a id="tocsv2glaccountlistresponse"></a>

```json
{
  "data": {
    "accounts": [
      {
        "id": "497f6eca-6276-4993-bfeb-53cbbbba6f08",
        "code": "string",
        "name": "string",
        "type": "asset",
        "normal_side": "debit",
        "currency": "string",
        "parent_code": "string",
        "ledger": "string",
        "address": "string",
        "status": "active",
        "metadata": {},
        "created_at": "2019-08-24T14:15:22Z",
        "updated_at": "2019-08-24T14:15:22Z"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|object|true|none|none|
|» accounts|[[V2GLAccount](#schemav2glaccount)]|true|none|none|

<h2 id="tocS_V2GLImportRowReport">V2GLImportRowReport</h2>
<!-- backwards compatibility -->
<a id="schemav2glimportrowreport"></a>
<a id="schema_V2GLImportRowReport"></a>
<a id="tocSv2glimportrowreport"></a>
<a id="tocsv2glimportrowreport"></a>

```json
{
  "row": 0,
  "code": "string",
  "status": "create",
  "errors": [
    "string"
  ],
  "conflicts": [
    "string"
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|row|integer|true|none|1-based position in the request.|
|code|string|true|none|none|
|status|string|true|none|none|
|errors|[string]|false|none|none|
|conflicts|[string]|false|none|Fields that differ from the existing account with the same code (or the clash on its address).|

#### Enumerated Values

|Property|Value|
|---|---|
|status|create|
|status|unchanged|
|status|conflict|
|status|invalid|

<h2 id="tocS_V2GLImportReport">V2GLImportReport</h2>
<!-- backwards compatibility -->
<a id="schemav2glimportreport"></a>
<a id="schema_V2GLImportReport"></a>
<a id="tocSv2glimportreport"></a>
<a id="tocsv2glimportreport"></a>

```json
{
  "dry_run": true,
  "committed": true,
  "can_commit": true,
  "total": 0,
  "to_create": 0,
  "unchanged": 0,
  "conflicts": 0,
  "invalid": 0,
  "rows": [
    {
      "row": 0,
      "code": "string",
      "status": "create",
      "errors": [
        "string"
      ],
      "conflicts": [
        "string"
      ]
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|dry_run|boolean|true|none|none|
|committed|boolean|true|none|True only when the import was applied (never for a dry run).|
|can_commit|boolean|true|none|No invalid row and no conflict.|
|total|integer|true|none|none|
|to_create|integer|true|none|none|
|unchanged|integer|true|none|none|
|conflicts|integer|true|none|none|
|invalid|integer|true|none|none|
|rows|[[V2GLImportRowReport](#schemav2glimportrowreport)]|true|none|none|

<h2 id="tocS_V2GLImportReportResponse">V2GLImportReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2glimportreportresponse"></a>
<a id="schema_V2GLImportReportResponse"></a>
<a id="tocSv2glimportreportresponse"></a>
<a id="tocsv2glimportreportresponse"></a>

```json
{
  "data": {
    "dry_run": true,
    "committed": true,
    "can_commit": true,
    "total": 0,
    "to_create": 0,
    "unchanged": 0,
    "conflicts": 0,
    "invalid": 0,
    "rows": [
      {
        "row": 0,
        "code": "string",
        "status": "create",
        "errors": [
          "string"
        ],
        "conflicts": [
          "string"
        ]
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2GLImportReport](#schemav2glimportreport)|true|none|none|

<h2 id="tocS_V2GLImportRejectedResponse">V2GLImportRejectedResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2glimportrejectedresponse"></a>
<a id="schema_V2GLImportRejectedResponse"></a>
<a id="tocSv2glimportrejectedresponse"></a>
<a id="tocsv2glimportrejectedresponse"></a>

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "string",
  "details": "string",
  "report": {
    "dry_run": true,
    "committed": true,
    "can_commit": true,
    "total": 0,
    "to_create": 0,
    "unchanged": 0,
    "conflicts": 0,
    "invalid": 0,
    "rows": [
      {
        "row": 0,
        "code": "string",
        "status": "create",
        "errors": [
          "string"
        ],
        "conflicts": [
          "string"
        ]
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|errorCode|string|true|none|none|
|errorMessage|string|true|none|none|
|details|string|false|none|none|
|report|[V2GLImportReport](#schemav2glimportreport)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|errorCode|VALIDATION|
|errorCode|CONFLICT|

<h2 id="tocS_V2GLAccountBalance">V2GLAccountBalance</h2>
<!-- backwards compatibility -->
<a id="schemav2glaccountbalance"></a>
<a id="schema_V2GLAccountBalance"></a>
<a id="tocSv2glaccountbalance"></a>
<a id="tocsv2glaccountbalance"></a>

```json
{
  "code": "string",
  "name": "string",
  "type": "asset",
  "normal_side": "debit",
  "currency": "string",
  "parent_code": "string",
  "ledger": "string",
  "address": "string",
  "status": "active",
  "asset": "NGN/2",
  "pit": "2019-08-24T14:15:22Z",
  "balance": 0,
  "debit": 0,
  "credit": 0,
  "natural_balance": 0,
  "abnormal": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|code|string|true|none|none|
|name|string|false|none|none|
|type|[V2GLAccountType](#schemav2glaccounttype)|false|none|none|
|normal_side|[V2GLSide](#schemav2glside)|false|none|none|
|currency|string|true|none|none|
|parent_code|string¦null|false|none|none|
|ledger|string|false|none|none|
|address|string|false|none|none|
|status|[V2GLAccountStatus](#schemav2glaccountstatus)|false|none|none|
|asset|string|true|none|none|
|pit|string(date-time)|false|none|none|
|balance|integer(int64)|true|none|Raw ledger balance (input - output) in minor units.|
|debit|integer(int64)|true|none|none|
|credit|integer(int64)|true|none|none|
|natural_balance|integer(int64)|true|none|Positive when the balance sits on the account's normal side.|
|abnormal|boolean|true|none|none|

<h2 id="tocS_V2GLAccountBalanceResponse">V2GLAccountBalanceResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2glaccountbalanceresponse"></a>
<a id="schema_V2GLAccountBalanceResponse"></a>
<a id="tocSv2glaccountbalanceresponse"></a>
<a id="tocsv2glaccountbalanceresponse"></a>

```json
{
  "data": {
    "code": "string",
    "name": "string",
    "type": "asset",
    "normal_side": "debit",
    "currency": "string",
    "parent_code": "string",
    "ledger": "string",
    "address": "string",
    "status": "active",
    "asset": "NGN/2",
    "pit": "2019-08-24T14:15:22Z",
    "balance": 0,
    "debit": 0,
    "credit": 0,
    "natural_balance": 0,
    "abnormal": true
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2GLAccountBalance](#schemav2glaccountbalance)|true|none|none|

<h2 id="tocS_V2GLTrialBalanceSubtotal">V2GLTrialBalanceSubtotal</h2>
<!-- backwards compatibility -->
<a id="schemav2gltrialbalancesubtotal"></a>
<a id="schema_V2GLTrialBalanceSubtotal"></a>
<a id="tocSv2gltrialbalancesubtotal"></a>
<a id="tocsv2gltrialbalancesubtotal"></a>

```json
{
  "type": "asset",
  "accounts": 0,
  "debit": 0,
  "credit": 0,
  "net": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|type|[V2GLAccountType](#schemav2glaccounttype)|true|none|none|
|accounts|integer|true|none|none|
|debit|integer(int64)|true|none|none|
|credit|integer(int64)|true|none|none|
|net|integer(int64)|true|none|debit - credit.|

<h2 id="tocS_V2GLTrialBalanceReport">V2GLTrialBalanceReport</h2>
<!-- backwards compatibility -->
<a id="schemav2gltrialbalancereport"></a>
<a id="schema_V2GLTrialBalanceReport"></a>
<a id="tocSv2gltrialbalancereport"></a>
<a id="tocsv2gltrialbalancereport"></a>

```json
{
  "currency": "string",
  "asset": "string",
  "as_of": "2019-08-24T14:15:22Z",
  "accounts": [
    {
      "code": "string",
      "name": "string",
      "type": "asset",
      "normal_side": "debit",
      "currency": "string",
      "parent_code": "string",
      "ledger": "string",
      "address": "string",
      "status": "active",
      "asset": "NGN/2",
      "pit": "2019-08-24T14:15:22Z",
      "balance": 0,
      "debit": 0,
      "credit": 0,
      "natural_balance": 0,
      "abnormal": true
    }
  ],
  "subtotals": [
    {
      "type": "asset",
      "accounts": 0,
      "debit": 0,
      "credit": 0,
      "net": 0
    }
  ],
  "total_debits": 0,
  "total_credits": 0,
  "difference": 0,
  "balanced": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|currency|string|true|none|none|
|asset|string|true|none|none|
|as_of|string(date-time)|false|none|none|
|accounts|[[V2GLAccountBalance](#schemav2glaccountbalance)]|true|none|none|
|subtotals|[[V2GLTrialBalanceSubtotal](#schemav2gltrialbalancesubtotal)]|true|none|One entry per account type, in the order asset, liability, equity, income, expense.|
|total_debits|integer(int64)|true|none|none|
|total_credits|integer(int64)|true|none|none|
|difference|integer(int64)|true|none|total_debits - total_credits.|
|balanced|boolean|true|none|none|

<h2 id="tocS_V2GLTrialBalanceReportResponse">V2GLTrialBalanceReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2gltrialbalancereportresponse"></a>
<a id="schema_V2GLTrialBalanceReportResponse"></a>
<a id="tocSv2gltrialbalancereportresponse"></a>
<a id="tocsv2gltrialbalancereportresponse"></a>

```json
{
  "data": {
    "currency": "string",
    "asset": "string",
    "as_of": "2019-08-24T14:15:22Z",
    "accounts": [
      {
        "code": "string",
        "name": "string",
        "type": "asset",
        "normal_side": "debit",
        "currency": "string",
        "parent_code": "string",
        "ledger": "string",
        "address": "string",
        "status": "active",
        "asset": "NGN/2",
        "pit": "2019-08-24T14:15:22Z",
        "balance": 0,
        "debit": 0,
        "credit": 0,
        "natural_balance": 0,
        "abnormal": true
      }
    ],
    "subtotals": [
      {
        "type": "asset",
        "accounts": 0,
        "debit": 0,
        "credit": 0,
        "net": 0
      }
    ],
    "total_debits": 0,
    "total_credits": 0,
    "difference": 0,
    "balanced": true
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2GLTrialBalanceReport](#schemav2gltrialbalancereport)|true|none|none|

<h2 id="tocS_V2FinanceBalanceSheetLine">V2FinanceBalanceSheetLine</h2>
<!-- backwards compatibility -->
<a id="schemav2financebalancesheetline"></a>
<a id="schema_V2FinanceBalanceSheetLine"></a>
<a id="tocSv2financebalancesheetline"></a>
<a id="tocsv2financebalancesheetline"></a>

```json
{
  "name": "string",
  "balance": 0,
  "currency": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|balance|integer(int64)|true|none|none|
|currency|string|true|none|none|

<h2 id="tocS_V2FinanceBalanceSheetReport">V2FinanceBalanceSheetReport</h2>
<!-- backwards compatibility -->
<a id="schemav2financebalancesheetreport"></a>
<a id="schema_V2FinanceBalanceSheetReport"></a>
<a id="tocSv2financebalancesheetreport"></a>
<a id="tocsv2financebalancesheetreport"></a>

```json
{
  "currency": "string",
  "as_of": "2019-08-24T14:15:22Z",
  "assets": [
    {
      "name": "string",
      "balance": 0,
      "currency": "string"
    }
  ],
  "liabilities": [
    {
      "name": "string",
      "balance": 0,
      "currency": "string"
    }
  ],
  "net_position": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|currency|string|true|none|none|
|as_of|string(date-time)|false|none|none|
|assets|[[V2FinanceBalanceSheetLine](#schemav2financebalancesheetline)]|true|none|none|
|liabilities|[[V2FinanceBalanceSheetLine](#schemav2financebalancesheetline)]|true|none|none|
|net_position|integer(int64)|true|none|none|

<h2 id="tocS_V2FinanceBalanceSheetReportResponse">V2FinanceBalanceSheetReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2financebalancesheetreportresponse"></a>
<a id="schema_V2FinanceBalanceSheetReportResponse"></a>
<a id="tocSv2financebalancesheetreportresponse"></a>
<a id="tocsv2financebalancesheetreportresponse"></a>

```json
{
  "data": {
    "currency": "string",
    "as_of": "2019-08-24T14:15:22Z",
    "assets": [
      {
        "name": "string",
        "balance": 0,
        "currency": "string"
      }
    ],
    "liabilities": [
      {
        "name": "string",
        "balance": 0,
        "currency": "string"
      }
    ],
    "net_position": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2FinanceBalanceSheetReport](#schemav2financebalancesheetreport)|true|none|none|

<h2 id="tocS_V2FinanceProfitAndLossLine">V2FinanceProfitAndLossLine</h2>
<!-- backwards compatibility -->
<a id="schemav2financeprofitandlossline"></a>
<a id="schema_V2FinanceProfitAndLossLine"></a>
<a id="tocSv2financeprofitandlossline"></a>
<a id="tocsv2financeprofitandlossline"></a>

```json
{
  "name": "string",
  "amount": 0,
  "currency": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|amount|integer(int64)|true|none|none|
|currency|string|true|none|none|

<h2 id="tocS_V2FinanceProfitAndLossReport">V2FinanceProfitAndLossReport</h2>
<!-- backwards compatibility -->
<a id="schemav2financeprofitandlossreport"></a>
<a id="schema_V2FinanceProfitAndLossReport"></a>
<a id="tocSv2financeprofitandlossreport"></a>
<a id="tocsv2financeprofitandlossreport"></a>

```json
{
  "currency": "string",
  "start_time": "2019-08-24T14:15:22Z",
  "end_time": "2019-08-24T14:15:22Z",
  "income": [
    {
      "name": "string",
      "amount": 0,
      "currency": "string"
    }
  ],
  "expenses": [
    {
      "name": "string",
      "amount": 0,
      "currency": "string"
    }
  ],
  "net_profit": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|currency|string|true|none|none|
|start_time|string(date-time)|false|none|none|
|end_time|string(date-time)|false|none|none|
|income|[[V2FinanceProfitAndLossLine](#schemav2financeprofitandlossline)]|true|none|none|
|expenses|[[V2FinanceProfitAndLossLine](#schemav2financeprofitandlossline)]|true|none|none|
|net_profit|integer(int64)|true|none|none|

<h2 id="tocS_V2FinanceProfitAndLossReportResponse">V2FinanceProfitAndLossReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2financeprofitandlossreportresponse"></a>
<a id="schema_V2FinanceProfitAndLossReportResponse"></a>
<a id="tocSv2financeprofitandlossreportresponse"></a>
<a id="tocsv2financeprofitandlossreportresponse"></a>

```json
{
  "data": {
    "currency": "string",
    "start_time": "2019-08-24T14:15:22Z",
    "end_time": "2019-08-24T14:15:22Z",
    "income": [
      {
        "name": "string",
        "amount": 0,
        "currency": "string"
      }
    ],
    "expenses": [
      {
        "name": "string",
        "amount": 0,
        "currency": "string"
      }
    ],
    "net_profit": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2FinanceProfitAndLossReport](#schemav2financeprofitandlossreport)|true|none|none|

<h2 id="tocS_V2FinanceCashFlowLine">V2FinanceCashFlowLine</h2>
<!-- backwards compatibility -->
<a id="schemav2financecashflowline"></a>
<a id="schema_V2FinanceCashFlowLine"></a>
<a id="tocSv2financecashflowline"></a>
<a id="tocsv2financecashflowline"></a>

```json
{
  "name": "string",
  "opening": 0,
  "closing": 0,
  "net_change": 0,
  "currency": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|opening|integer(int64)|true|none|none|
|closing|integer(int64)|true|none|none|
|net_change|integer(int64)|true|none|none|
|currency|string|true|none|none|

<h2 id="tocS_V2FinanceCashFlowReport">V2FinanceCashFlowReport</h2>
<!-- backwards compatibility -->
<a id="schemav2financecashflowreport"></a>
<a id="schema_V2FinanceCashFlowReport"></a>
<a id="tocSv2financecashflowreport"></a>
<a id="tocsv2financecashflowreport"></a>

```json
{
  "currency": "string",
  "start_time": "2019-08-24T14:15:22Z",
  "end_time": "2019-08-24T14:15:22Z",
  "lines": [
    {
      "name": "string",
      "opening": 0,
      "closing": 0,
      "net_change": 0,
      "currency": "string"
    }
  ]
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|currency|string|true|none|none|
|start_time|string(date-time)|false|none|none|
|end_time|string(date-time)|false|none|none|
|lines|[[V2FinanceCashFlowLine](#schemav2financecashflowline)]|true|none|none|

<h2 id="tocS_V2FinanceCashFlowReportResponse">V2FinanceCashFlowReportResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2financecashflowreportresponse"></a>
<a id="schema_V2FinanceCashFlowReportResponse"></a>
<a id="tocSv2financecashflowreportresponse"></a>
<a id="tocsv2financecashflowreportresponse"></a>

```json
{
  "data": {
    "currency": "string",
    "start_time": "2019-08-24T14:15:22Z",
    "end_time": "2019-08-24T14:15:22Z",
    "lines": [
      {
        "name": "string",
        "opening": 0,
        "closing": 0,
        "net_change": 0,
        "currency": "string"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2FinanceCashFlowReport](#schemav2financecashflowreport)|true|none|none|

<h2 id="tocS_V2Posting">V2Posting</h2>
<!-- backwards compatibility -->
<a id="schemav2posting"></a>
<a id="schema_V2Posting"></a>
<a id="tocSv2posting"></a>
<a id="tocsv2posting"></a>

```json
{
  "amount": 100,
  "asset": "COIN",
  "destination": "users:002",
  "source": "users:001"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|amount|integer(bigint)|true|none|none|
|asset|string|true|none|none|
|destination|string|true|none|none|
|source|string|true|none|none|

<h2 id="tocS_V2Transaction">V2Transaction</h2>
<!-- backwards compatibility -->
<a id="schemav2transaction"></a>
<a id="schema_V2Transaction"></a>
<a id="tocSv2transaction"></a>
<a id="tocsv2transaction"></a>

```json
{
  "insertedAt": "2019-08-24T14:15:22Z",
  "updatedAt": "2019-08-24T14:15:22Z",
  "timestamp": "2019-08-24T14:15:22Z",
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "reference": "ref:001",
  "metadata": {
    "admin": "true"
  },
  "id": 0,
  "reverted": true,
  "revertedAt": "2019-08-24T14:15:22Z",
  "preCommitVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "postCommitVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "preCommitEffectiveVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "postCommitEffectiveVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "template": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|insertedAt|string(date-time)|false|none|none|
|updatedAt|string(date-time)|false|none|none|
|timestamp|string(date-time)|true|none|none|
|postings|[[V2Posting](#schemav2posting)]|true|none|none|
|reference|string|false|none|none|
|metadata|[V2Metadata](#schemav2metadata)|true|none|none|
|id|integer(bigint)|true|none|none|
|reverted|boolean|true|none|none|
|revertedAt|string(date-time)|false|none|none|
|preCommitVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|postCommitVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|preCommitEffectiveVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|postCommitEffectiveVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|template|string|false|none|none|

<h2 id="tocS_V2PostTransaction">V2PostTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2posttransaction"></a>
<a id="schema_V2PostTransaction"></a>
<a id="tocSv2posttransaction"></a>
<a id="tocsv2posttransaction"></a>

```json
{
  "timestamp": "2019-08-24T14:15:22Z",
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "script": {
    "template": "CUSTOMER_DEPOSIT",
    "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
    "vars": {
      "user": "users:042"
    }
  },
  "runtime": "experimental-interpreter",
  "reference": "ref:001",
  "metadata": {
    "admin": "true"
  },
  "accountMetadata": {
    "property1": {
      "admin": "true"
    },
    "property2": {
      "admin": "true"
    }
  },
  "force": true
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|timestamp|string(date-time)|false|none|none|
|postings|[[V2Posting](#schemav2posting)]|false|none|none|
|script|object|false|none|none|
|» template|string|false|none|none|
|» plain|string|false|none|none|
|» vars|object|false|none|none|
|»» **additionalProperties**|string|false|none|none|
|runtime|[Runtime](#schemaruntime)|false|none|The numscript runtime used to execute the script. Uses "machine" by default, unless the "--experimental-numscript-interpreter" feature flag is passed.|
|reference|string|false|none|none|
|metadata|[V2Metadata](#schemav2metadata)|true|none|none|
|accountMetadata|object|false|none|none|
|» **additionalProperties**|[V2Metadata](#schemav2metadata)|false|none|none|
|force|boolean|false|none|none|

<h2 id="tocS_V2Stats">V2Stats</h2>
<!-- backwards compatibility -->
<a id="schemav2stats"></a>
<a id="schema_V2Stats"></a>
<a id="tocSv2stats"></a>
<a id="tocsv2stats"></a>

```json
{
  "accounts": 0,
  "transactions": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|accounts|integer(int64)|true|none|none|
|transactions|integer(bigint)|true|none|none|

<h2 id="tocS_V2Log">V2Log</h2>
<!-- backwards compatibility -->
<a id="schemav2log"></a>
<a id="schema_V2Log"></a>
<a id="tocSv2log"></a>
<a id="tocsv2log"></a>

```json
{
  "id": 1234,
  "type": "NEW_TRANSACTION",
  "data": {
    "transaction": {
      "id": 1234,
      "postings": [
        {
          "source": "world",
          "destination": "users:001",
          "amount": 100,
          "asset": "USD/2"
        }
      ],
      "metadata": {},
      "timestamp": "2024-01-15T10:30:00Z",
      "insertedAt": "2024-01-15T10:30:00Z",
      "reverted": false
    },
    "accountMetadata": {
      "users:001": {
        "created_by": "system"
      }
    }
  },
  "hash": "9ee060170400f556b7e1575cb13f9db004f150a08355c7431c62bc639166431e",
  "date": "2019-08-24T14:15:22Z",
  "schemaVersion": "v1.0.0"
}

```

Represents an immutable log entry in the ledger. Each log captures an atomic operation
with its full payload, enabling audit trails and event sourcing patterns.
The data field structure depends on the log type.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer(bigint)|true|none|Unique sequential identifier for this log entry within the ledger|
|type|string|true|none|The type of operation this log represents|
|data|any|true|none|The payload of the log entry. Structure depends on the log type:<br>- NEW_TRANSACTION: V2LogDataNewTransaction<br>- SET_METADATA: V2LogDataSetMetadata<br>- REVERTED_TRANSACTION: V2LogDataRevertedTransaction<br>- DELETE_METADATA: V2LogDataDeleteMetadata<br>- INSERTED_SCHEMA: V2LogDataInsertedSchema|

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[V2LogDataNewTransaction](#schemav2logdatanewtransaction)|false|none|Payload for NEW_TRANSACTION log entries. Contains the created transaction and any account metadata set during creation.|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[V2LogDataSetMetadata](#schemav2logdatasetmetadata)|false|none|Payload for SET_METADATA log entries. Contains the target entity and the metadata that was set.|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[V2LogDataRevertedTransaction](#schemav2logdatarevertedtransaction)|false|none|Payload for REVERTED_TRANSACTION log entries. Contains both the original reverted transaction and the new reverting transaction.|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[V2LogDataDeleteMetadata](#schemav2logdatadeletemetadata)|false|none|Payload for DELETE_METADATA log entries. Contains the target entity and the metadata key that was deleted.|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|[V2LogDataInsertedSchema](#schemav2logdatainsertedschema)|false|none|Payload for INSERTED_SCHEMA log entries. Contains the schema that was inserted into the ledger.|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|hash|string|true|none|SHA256 hash of the log entry, chained from the previous log for integrity verification|
|date|string(date-time)|true|none|Timestamp when the operation was recorded|
|schemaVersion|string|false|none|Schema version used for validation when the log was created|

#### Enumerated Values

|Property|Value|
|---|---|
|type|NEW_TRANSACTION|
|type|SET_METADATA|
|type|REVERTED_TRANSACTION|
|type|DELETE_METADATA|
|type|INSERTED_SCHEMA|

<h2 id="tocS_V2LogTransaction">V2LogTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2logtransaction"></a>
<a id="schema_V2LogTransaction"></a>
<a id="tocSv2logtransaction"></a>
<a id="tocsv2logtransaction"></a>

```json
{
  "id": 0,
  "postings": [
    {
      "amount": 100,
      "asset": "COIN",
      "destination": "users:002",
      "source": "users:001"
    }
  ],
  "metadata": {
    "admin": "true"
  },
  "timestamp": "2019-08-24T14:15:22Z",
  "reference": "string",
  "insertedAt": "2019-08-24T14:15:22Z",
  "updatedAt": "2019-08-24T14:15:22Z",
  "revertedAt": "2019-08-24T14:15:22Z",
  "reverted": true,
  "template": "string",
  "postCommitVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "postCommitEffectiveVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "preCommitVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  },
  "preCommitEffectiveVolumes": {
    "orders:1": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    },
    "orders:2": {
      "USD": {
        "input": 100,
        "output": 10,
        "balance": 90
      }
    }
  }
}

```

Transaction structure as it appears in log payloads

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|id|integer(bigint)|true|none|none|
|postings|[[V2Posting](#schemav2posting)]|true|none|none|
|metadata|[V2Metadata](#schemav2metadata)|true|none|none|
|timestamp|string(date-time)|true|none|none|
|reference|string|false|none|none|
|insertedAt|string(date-time)|false|none|none|
|updatedAt|string(date-time)|false|none|none|
|revertedAt|string(date-time)|false|none|none|
|reverted|boolean|true|none|Indicates if the transaction has been reverted|
|template|string|false|none|Transaction template used|
|postCommitVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|postCommitEffectiveVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|preCommitVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|
|preCommitEffectiveVolumes|[V2AggregatedVolumes](#schemav2aggregatedvolumes)|false|none|none|

<h2 id="tocS_V2LogDataNewTransaction">V2LogDataNewTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2logdatanewtransaction"></a>
<a id="schema_V2LogDataNewTransaction"></a>
<a id="tocSv2logdatanewtransaction"></a>
<a id="tocsv2logdatanewtransaction"></a>

```json
{
  "transaction": {
    "id": 1234,
    "postings": [
      {
        "source": "world",
        "destination": "users:001",
        "amount": 100,
        "asset": "USD/2"
      }
    ],
    "metadata": {},
    "timestamp": "2024-01-15T10:30:00Z",
    "insertedAt": "2024-01-15T10:30:00Z",
    "reverted": false
  },
  "accountMetadata": {
    "users:001": {
      "created_by": "system"
    }
  }
}

```

Payload for NEW_TRANSACTION log entries. Contains the created transaction and any account metadata set during creation.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|transaction|[V2LogTransaction](#schemav2logtransaction)|true|none|Transaction structure as it appears in log payloads|
|accountMetadata|object|true|none|Metadata applied to accounts involved in the transaction|
|» **additionalProperties**|[V2Metadata](#schemav2metadata)|false|none|none|

<h2 id="tocS_V2LogDataSetMetadata">V2LogDataSetMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2logdatasetmetadata"></a>
<a id="schema_V2LogDataSetMetadata"></a>
<a id="tocSv2logdatasetmetadata"></a>
<a id="tocsv2logdatasetmetadata"></a>

```json
{
  "targetType": "ACCOUNT",
  "targetId": "users:001",
  "metadata": {
    "status": "active",
    "tier": "premium"
  }
}

```

Payload for SET_METADATA log entries. Contains the target entity and the metadata that was set.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|targetType|string|true|none|Type of the target entity|
|targetId|any|true|none|none|

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|string|false|none|Account address (when targetType is ACCOUNT)|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|integer(bigint)|false|none|Transaction ID (when targetType is TRANSACTION)|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|metadata|[V2Metadata](#schemav2metadata)|true|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|targetType|ACCOUNT|
|targetType|TRANSACTION|

<h2 id="tocS_V2LogDataRevertedTransaction">V2LogDataRevertedTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2logdatarevertedtransaction"></a>
<a id="schema_V2LogDataRevertedTransaction"></a>
<a id="tocSv2logdatarevertedtransaction"></a>
<a id="tocsv2logdatarevertedtransaction"></a>

```json
{
  "revertedTransaction": {
    "id": 1234,
    "postings": [
      {
        "source": "world",
        "destination": "users:001",
        "amount": 100,
        "asset": "USD/2"
      }
    ],
    "metadata": {},
    "timestamp": "2024-01-15T10:30:00Z",
    "reverted": true
  },
  "transaction": {
    "id": 1235,
    "postings": [
      {
        "source": "users:001",
        "destination": "world",
        "amount": 100,
        "asset": "USD/2"
      }
    ],
    "metadata": {
      "revert": "1234"
    },
    "timestamp": "2024-01-15T11:00:00Z",
    "reverted": false
  }
}

```

Payload for REVERTED_TRANSACTION log entries. Contains both the original reverted transaction and the new reverting transaction.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|revertedTransaction|[V2LogTransaction](#schemav2logtransaction)|true|none|The original transaction that was reverted|
|transaction|[V2LogTransaction](#schemav2logtransaction)|true|none|The new reverting transaction created to cancel the original|

<h2 id="tocS_V2LogDataDeleteMetadata">V2LogDataDeleteMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2logdatadeletemetadata"></a>
<a id="schema_V2LogDataDeleteMetadata"></a>
<a id="tocSv2logdatadeletemetadata"></a>
<a id="tocsv2logdatadeletemetadata"></a>

```json
{
  "targetType": "ACCOUNT",
  "targetId": "users:001",
  "key": "temporary_flag"
}

```

Payload for DELETE_METADATA log entries. Contains the target entity and the metadata key that was deleted.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|targetType|string|true|none|Type of the target entity|
|targetId|any|true|none|none|

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|string|false|none|Account address (when targetType is ACCOUNT)|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|» *anonymous*|integer(bigint)|false|none|Transaction ID (when targetType is TRANSACTION)|

continued

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|key|string|true|none|The metadata key that was deleted|

#### Enumerated Values

|Property|Value|
|---|---|
|targetType|ACCOUNT|
|targetType|TRANSACTION|

<h2 id="tocS_V2LogDataInsertedSchema">V2LogDataInsertedSchema</h2>
<!-- backwards compatibility -->
<a id="schemav2logdatainsertedschema"></a>
<a id="schema_V2LogDataInsertedSchema"></a>
<a id="tocSv2logdatainsertedschema"></a>
<a id="tocsv2logdatainsertedschema"></a>

```json
{
  "schema": {
    "version": "v1.0.0",
    "createdAt": "2023-01-01T00:00:00Z",
    "chart": {
      "users": {
        "$userID": {
          ".pattern": "^[0-9]{16}$"
        }
      }
    },
    "transactions": {
      "property1": {
        "description": "string",
        "script": "string",
        "runtime": "experimental-interpreter"
      },
      "property2": {
        "description": "string",
        "script": "string",
        "runtime": "experimental-interpreter"
      }
    }
  }
}

```

Payload for INSERTED_SCHEMA log entries. Contains the schema that was inserted into the ledger.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|schema|[V2Schema](#schemav2schema)|true|none|Complete schema structure with metadata|

<h2 id="tocS_V2CreateTransactionResponse">V2CreateTransactionResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2createtransactionresponse"></a>
<a id="schema_V2CreateTransactionResponse"></a>
<a id="tocSv2createtransactionresponse"></a>
<a id="tocsv2createtransactionresponse"></a>

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2RevertTransactionResponse">V2RevertTransactionResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2reverttransactionresponse"></a>
<a id="schema_V2RevertTransactionResponse"></a>
<a id="tocSv2reverttransactionresponse"></a>
<a id="tocsv2reverttransactionresponse"></a>

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}

```

### Properties

*None*

<h2 id="tocS_V2GetTransactionResponse">V2GetTransactionResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2gettransactionresponse"></a>
<a id="schema_V2GetTransactionResponse"></a>
<a id="tocSv2gettransactionresponse"></a>
<a id="tocsv2gettransactionresponse"></a>

```json
{
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2StatsResponse">V2StatsResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2statsresponse"></a>
<a id="schema_V2StatsResponse"></a>
<a id="tocSv2statsresponse"></a>
<a id="tocsv2statsresponse"></a>

```json
{
  "data": {
    "accounts": 0,
    "transactions": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Stats](#schemav2stats)|true|none|none|

<h2 id="tocS_V2ConfigInfoResponse">V2ConfigInfoResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2configinforesponse"></a>
<a id="schema_V2ConfigInfoResponse"></a>
<a id="tocSv2configinforesponse"></a>
<a id="tocsv2configinforesponse"></a>

```json
{
  "server": "string",
  "version": "string"
}

```

### Properties

*None*

<h2 id="tocS_V2Volume">V2Volume</h2>
<!-- backwards compatibility -->
<a id="schemav2volume"></a>
<a id="schema_V2Volume"></a>
<a id="tocSv2volume"></a>
<a id="tocsv2volume"></a>

```json
{
  "input": 100,
  "output": 20,
  "balance": 80
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|input|integer(bigint)|true|none|none|
|output|integer(bigint)|true|none|none|
|balance|integer(bigint)|false|none|none|

<h2 id="tocS_V2Volumes">V2Volumes</h2>
<!-- backwards compatibility -->
<a id="schemav2volumes"></a>
<a id="schema_V2Volumes"></a>
<a id="tocSv2volumes"></a>
<a id="tocsv2volumes"></a>

```json
{
  "USD": {
    "input": 100,
    "output": 10,
    "balance": 90
  },
  "EUR": {
    "input": 100,
    "output": 10,
    "balance": 90
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|[V2Volume](#schemav2volume)|false|none|none|

<h2 id="tocS_V2AggregatedVolumes">V2AggregatedVolumes</h2>
<!-- backwards compatibility -->
<a id="schemav2aggregatedvolumes"></a>
<a id="schema_V2AggregatedVolumes"></a>
<a id="tocSv2aggregatedvolumes"></a>
<a id="tocsv2aggregatedvolumes"></a>

```json
{
  "orders:1": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  },
  "orders:2": {
    "USD": {
      "input": 100,
      "output": 10,
      "balance": 90
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|[V2Volumes](#schemav2volumes)|false|none|none|

<h2 id="tocS_V2ErrorResponse">V2ErrorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2errorresponse"></a>
<a id="schema_V2ErrorResponse"></a>
<a id="tocSv2errorresponse"></a>
<a id="tocsv2errorresponse"></a>

```json
{
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param",
  "details": "https://play.numscript.org/?payload=eyJlcnJvciI6ImFjY291bnQgaGFkIGluc3VmZmljaWVudCBmdW5kcyJ9"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|errorCode|[V2ErrorsEnum](#schemav2errorsenum)|true|none|none|
|errorMessage|string|true|none|none|
|details|string|false|none|none|

<h2 id="tocS_V2ErrorsEnum">V2ErrorsEnum</h2>
<!-- backwards compatibility -->
<a id="schemav2errorsenum"></a>
<a id="schema_V2ErrorsEnum"></a>
<a id="tocSv2errorsenum"></a>
<a id="tocsv2errorsenum"></a>

```json
"VALIDATION"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|INTERNAL|
|*anonymous*|INSUFFICIENT_FUND|
|*anonymous*|VALIDATION|
|*anonymous*|CONFLICT|
|*anonymous*|COMPILATION_FAILED|
|*anonymous*|METADATA_OVERRIDE|
|*anonymous*|NOT_FOUND|
|*anonymous*|REVERT_OCCURRING|
|*anonymous*|ALREADY_REVERT|
|*anonymous*|NO_POSTINGS|
|*anonymous*|LEDGER_NOT_FOUND|
|*anonymous*|IMPORT|
|*anonymous*|TIMEOUT|
|*anonymous*|BULK_SIZE_EXCEEDED|
|*anonymous*|INTERPRETER_PARSE|
|*anonymous*|INTERPRETER_RUNTIME|
|*anonymous*|LEDGER_ALREADY_EXISTS|
|*anonymous*|SCHEMA_ALREADY_EXISTS|
|*anonymous*|SCHEMA_NOT_SPECIFIED|
|*anonymous*|OUTDATED_SCHEMA|

<h2 id="tocS_V2LedgerInfoResponse">V2LedgerInfoResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2ledgerinforesponse"></a>
<a id="schema_V2LedgerInfoResponse"></a>
<a id="tocSv2ledgerinforesponse"></a>
<a id="tocsv2ledgerinforesponse"></a>

```json
{
  "data": {
    "name": "ledger001",
    "storage": {
      "migrations": [
        {
          "version": 11,
          "name": "migrations:001",
          "date": "2019-08-24T14:15:22Z",
          "state": "TO DO"
        }
      ]
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2LedgerInfo](#schemav2ledgerinfo)|false|none|none|

<h2 id="tocS_V2LedgerInfo">V2LedgerInfo</h2>
<!-- backwards compatibility -->
<a id="schemav2ledgerinfo"></a>
<a id="schema_V2LedgerInfo"></a>
<a id="tocSv2ledgerinfo"></a>
<a id="tocsv2ledgerinfo"></a>

```json
{
  "name": "ledger001",
  "storage": {
    "migrations": [
      {
        "version": 11,
        "name": "migrations:001",
        "date": "2019-08-24T14:15:22Z",
        "state": "TO DO"
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|false|none|none|
|storage|object|false|none|none|
|» migrations|[[V2MigrationInfo](#schemav2migrationinfo)]|false|none|none|

<h2 id="tocS_V2MigrationInfo">V2MigrationInfo</h2>
<!-- backwards compatibility -->
<a id="schemav2migrationinfo"></a>
<a id="schema_V2MigrationInfo"></a>
<a id="tocSv2migrationinfo"></a>
<a id="tocsv2migrationinfo"></a>

```json
{
  "version": 11,
  "name": "migrations:001",
  "date": "2019-08-24T14:15:22Z",
  "state": "TO DO"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|version|string|false|none|none|
|name|string|false|none|none|
|date|string(date-time)|false|none|none|
|state|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|state|TO DO|
|state|DONE|
|state|PROGRESS|

<h2 id="tocS_V2Bulk">V2Bulk</h2>
<!-- backwards compatibility -->
<a id="schemav2bulk"></a>
<a id="schema_V2Bulk"></a>
<a id="tocSv2bulk"></a>
<a id="tocsv2bulk"></a>

```json
[
  {
    "action": "string",
    "ik": "string",
    "data": {
      "timestamp": "2019-08-24T14:15:22Z",
      "postings": [
        {
          "amount": 100,
          "asset": "COIN",
          "destination": "users:002",
          "source": "users:001"
        }
      ],
      "script": {
        "template": "CUSTOMER_DEPOSIT",
        "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
        "vars": {
          "user": "users:042"
        }
      },
      "runtime": "experimental-interpreter",
      "reference": "ref:001",
      "metadata": {
        "admin": "true"
      },
      "accountMetadata": {
        "property1": {
          "admin": "true"
        },
        "property2": {
          "admin": "true"
        }
      },
      "force": true
    }
  }
]

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[[V2BulkElement](#schemav2bulkelement)]|false|none|none|

<h2 id="tocS_V2BaseBulkElement">V2BaseBulkElement</h2>
<!-- backwards compatibility -->
<a id="schemav2basebulkelement"></a>
<a id="schema_V2BaseBulkElement"></a>
<a id="tocSv2basebulkelement"></a>
<a id="tocsv2basebulkelement"></a>

```json
{
  "action": "string",
  "ik": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|action|string|true|none|none|
|ik|string|false|none|none|

<h2 id="tocS_V2BulkElement">V2BulkElement</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelement"></a>
<a id="schema_V2BulkElement"></a>
<a id="tocSv2bulkelement"></a>
<a id="tocsv2bulkelement"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "script": {
      "template": "CUSTOMER_DEPOSIT",
      "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
      "vars": {
        "user": "users:042"
      }
    },
    "runtime": "experimental-interpreter",
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "accountMetadata": {
      "property1": {
        "admin": "true"
      },
      "property2": {
        "admin": "true"
      }
    },
    "force": true
  }
}

```

### Properties

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementCreateTransaction](#schemav2bulkelementcreatetransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementAddMetadata](#schemav2bulkelementaddmetadata)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementRevertTransaction](#schemav2bulkelementreverttransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementDeleteMetadata](#schemav2bulkelementdeletemetadata)|false|none|none|

<h2 id="tocS_V2BulkElementCreateTransaction">V2BulkElementCreateTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementcreatetransaction"></a>
<a id="schema_V2BulkElementCreateTransaction"></a>
<a id="tocSv2bulkelementcreatetransaction"></a>
<a id="tocsv2bulkelementcreatetransaction"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "script": {
      "template": "CUSTOMER_DEPOSIT",
      "plain": "vars {\naccount $user\n}\nsend [COIN 10] (\n\tsource = @world\n\tdestination = $user\n)\n",
      "vars": {
        "user": "users:042"
      }
    },
    "runtime": "experimental-interpreter",
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "accountMetadata": {
      "property1": {
        "admin": "true"
      },
      "property2": {
        "admin": "true"
      }
    },
    "force": true
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|[V2PostTransaction](#schemav2posttransaction)|false|none|none|

<h2 id="tocS_V2TargetId">V2TargetId</h2>
<!-- backwards compatibility -->
<a id="schemav2targetid"></a>
<a id="schema_V2TargetId"></a>
<a id="tocSv2targetid"></a>
<a id="tocsv2targetid"></a>

```json
"string"

```

### Properties

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|integer(bigint)|false|none|none|

<h2 id="tocS_V2TargetType">V2TargetType</h2>
<!-- backwards compatibility -->
<a id="schemav2targettype"></a>
<a id="schema_V2TargetType"></a>
<a id="tocSv2targettype"></a>
<a id="tocsv2targettype"></a>

```json
"TRANSACTION"

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|none|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|TRANSACTION|
|*anonymous*|ACCOUNT|

<h2 id="tocS_V2BulkElementAddMetadata">V2BulkElementAddMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementaddmetadata"></a>
<a id="schema_V2BulkElementAddMetadata"></a>
<a id="tocSv2bulkelementaddmetadata"></a>
<a id="tocsv2bulkelementaddmetadata"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "targetId": "string",
    "targetType": "TRANSACTION",
    "metadata": {
      "property1": "string",
      "property2": "string"
    }
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|object|false|none|none|
|»» targetId|[V2TargetId](#schemav2targetid)|true|none|none|
|»» targetType|[V2TargetType](#schemav2targettype)|true|none|none|
|»» metadata|object|true|none|none|
|»»» **additionalProperties**|string|false|none|none|

<h2 id="tocS_V2BulkElementRevertTransaction">V2BulkElementRevertTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementreverttransaction"></a>
<a id="schema_V2BulkElementRevertTransaction"></a>
<a id="tocSv2bulkelementreverttransaction"></a>
<a id="tocsv2bulkelementreverttransaction"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "id": 0,
    "force": true,
    "atEffectiveDate": true,
    "metadata": {
      "admin": "true"
    }
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|object|false|none|none|
|»» id|integer(bigint)|true|none|none|
|»» force|boolean|false|none|none|
|»» atEffectiveDate|boolean|false|none|none|
|»» metadata|[V2Metadata](#schemav2metadata)|false|none|none|

<h2 id="tocS_V2BulkElementDeleteMetadata">V2BulkElementDeleteMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementdeletemetadata"></a>
<a id="schema_V2BulkElementDeleteMetadata"></a>
<a id="tocSv2bulkelementdeletemetadata"></a>
<a id="tocsv2bulkelementdeletemetadata"></a>

```json
{
  "action": "string",
  "ik": "string",
  "data": {
    "targetId": "string",
    "targetType": "TRANSACTION",
    "key": "string"
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElement](#schemav2basebulkelement)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|object|false|none|none|
|»» targetId|[V2TargetId](#schemav2targetid)|true|none|none|
|»» targetType|[V2TargetType](#schemav2targettype)|true|none|none|
|»» key|string|true|none|none|

<h2 id="tocS_V2BulkResponse">V2BulkResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkresponse"></a>
<a id="schema_V2BulkResponse"></a>
<a id="tocSv2bulkresponse"></a>
<a id="tocsv2bulkresponse"></a>

```json
{
  "data": [
    {
      "responseType": "string",
      "logID": 0,
      "data": {
        "insertedAt": "2019-08-24T14:15:22Z",
        "updatedAt": "2019-08-24T14:15:22Z",
        "timestamp": "2019-08-24T14:15:22Z",
        "postings": [
          {
            "amount": 100,
            "asset": "COIN",
            "destination": "users:002",
            "source": "users:001"
          }
        ],
        "reference": "ref:001",
        "metadata": {
          "admin": "true"
        },
        "id": 0,
        "reverted": true,
        "revertedAt": "2019-08-24T14:15:22Z",
        "preCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "preCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "postCommitEffectiveVolumes": {
          "orders:1": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          },
          "orders:2": {
            "USD": {
              "input": 100,
              "output": 10,
              "balance": 90
            }
          }
        },
        "template": "string"
      }
    }
  ],
  "errorCode": "VALIDATION",
  "errorMessage": "[VALIDATION] invalid 'cursor' query param"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[[V2BulkElementResult](#schemav2bulkelementresult)]|false|none|none|
|errorCode|[V2ErrorsEnum](#schemav2errorsenum)|false|none|none|
|errorMessage|string|false|none|none|

<h2 id="tocS_V2BulkElementResult">V2BulkElementResult</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresult"></a>
<a id="schema_V2BulkElementResult"></a>
<a id="tocSv2bulkelementresult"></a>
<a id="tocsv2bulkelementresult"></a>

```json
{
  "responseType": "string",
  "logID": 0,
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}

```

### Properties

oneOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultCreateTransaction](#schemav2bulkelementresultcreatetransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultAddMetadata](#schemav2bulkelementresultaddmetadata)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultRevertTransaction](#schemav2bulkelementresultreverttransaction)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultDeleteMetadata](#schemav2bulkelementresultdeletemetadata)|false|none|none|

xor

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BulkElementResultError](#schemav2bulkelementresulterror)|false|none|none|

<h2 id="tocS_V2BaseBulkElementResult">V2BaseBulkElementResult</h2>
<!-- backwards compatibility -->
<a id="schemav2basebulkelementresult"></a>
<a id="schema_V2BaseBulkElementResult"></a>
<a id="tocSv2basebulkelementresult"></a>
<a id="tocsv2basebulkelementresult"></a>

```json
{
  "responseType": "string",
  "logID": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|responseType|string|true|none|none|
|logID|integer|true|none|none|

<h2 id="tocS_V2BulkElementResultCreateTransaction">V2BulkElementResultCreateTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultcreatetransaction"></a>
<a id="schema_V2BulkElementResultCreateTransaction"></a>
<a id="tocSv2bulkelementresultcreatetransaction"></a>
<a id="tocsv2bulkelementresultcreatetransaction"></a>

```json
{
  "responseType": "string",
  "logID": 0,
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElementResult](#schemav2basebulkelementresult)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2BulkElementResultAddMetadata">V2BulkElementResultAddMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultaddmetadata"></a>
<a id="schema_V2BulkElementResultAddMetadata"></a>
<a id="tocSv2bulkelementresultaddmetadata"></a>
<a id="tocsv2bulkelementresultaddmetadata"></a>

```json
{
  "responseType": "string",
  "logID": 0
}

```

### Properties

*None*

<h2 id="tocS_V2BulkElementResultRevertTransaction">V2BulkElementResultRevertTransaction</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultreverttransaction"></a>
<a id="schema_V2BulkElementResultRevertTransaction"></a>
<a id="tocSv2bulkelementresultreverttransaction"></a>
<a id="tocsv2bulkelementresultreverttransaction"></a>

```json
{
  "responseType": "string",
  "logID": 0,
  "data": {
    "insertedAt": "2019-08-24T14:15:22Z",
    "updatedAt": "2019-08-24T14:15:22Z",
    "timestamp": "2019-08-24T14:15:22Z",
    "postings": [
      {
        "amount": 100,
        "asset": "COIN",
        "destination": "users:002",
        "source": "users:001"
      }
    ],
    "reference": "ref:001",
    "metadata": {
      "admin": "true"
    },
    "id": 0,
    "reverted": true,
    "revertedAt": "2019-08-24T14:15:22Z",
    "preCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "preCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "postCommitEffectiveVolumes": {
      "orders:1": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      },
      "orders:2": {
        "USD": {
          "input": 100,
          "output": 10,
          "balance": 90
        }
      }
    },
    "template": "string"
  }
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElementResult](#schemav2basebulkelementresult)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» data|[V2Transaction](#schemav2transaction)|true|none|none|

<h2 id="tocS_V2BulkElementResultDeleteMetadata">V2BulkElementResultDeleteMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresultdeletemetadata"></a>
<a id="schema_V2BulkElementResultDeleteMetadata"></a>
<a id="tocSv2bulkelementresultdeletemetadata"></a>
<a id="tocsv2bulkelementresultdeletemetadata"></a>

```json
{
  "responseType": "string",
  "logID": 0
}

```

### Properties

*None*

<h2 id="tocS_V2BulkElementResultError">V2BulkElementResultError</h2>
<!-- backwards compatibility -->
<a id="schemav2bulkelementresulterror"></a>
<a id="schema_V2BulkElementResultError"></a>
<a id="tocSv2bulkelementresulterror"></a>
<a id="tocsv2bulkelementresulterror"></a>

```json
{
  "responseType": "string",
  "logID": 0,
  "errorCode": "string",
  "errorDescription": "string",
  "errorDetails": "string"
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2BaseBulkElementResult](#schemav2basebulkelementresult)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» errorCode|string|true|none|none|
|» errorDescription|string|true|none|none|
|» errorDetails|string|false|none|none|

<h2 id="tocS_V2ChartAccountRules">V2ChartAccountRules</h2>
<!-- backwards compatibility -->
<a id="schemav2chartaccountrules"></a>
<a id="schema_V2ChartAccountRules"></a>
<a id="tocSv2chartaccountrules"></a>
<a id="tocsv2chartaccountrules"></a>

```json
{}

```

### Properties

*None*

<h2 id="tocS_V2ChartAccountMetadata">V2ChartAccountMetadata</h2>
<!-- backwards compatibility -->
<a id="schemav2chartaccountmetadata"></a>
<a id="schema_V2ChartAccountMetadata"></a>
<a id="tocSv2chartaccountmetadata"></a>
<a id="tocsv2chartaccountmetadata"></a>

```json
{
  "default": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|default|string|false|none|none|

<h2 id="tocS_V2ChartSegment">V2ChartSegment</h2>
<!-- backwards compatibility -->
<a id="schemav2chartsegment"></a>
<a id="schema_V2ChartSegment"></a>
<a id="tocSv2chartsegment"></a>
<a id="tocsv2chartsegment"></a>

```json
{
  "users": {
    "$userID": {
      ".pattern": "^[0-9]{16}$"
    }
  }
}

```

Segment within a chart of accounts

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|[V2ChartSegment](#schemav2chartsegment)|false|none|Segment within a chart of accounts|
|.self|object|false|none|none|
|.pattern|string|false|none|none|
|.rules|[V2ChartAccountRules](#schemav2chartaccountrules)|false|none|none|
|.metadata|object|false|none|none|
|» **additionalProperties**|[V2ChartAccountMetadata](#schemav2chartaccountmetadata)|false|none|none|

<h2 id="tocS_V2ChartOfAccounts">V2ChartOfAccounts</h2>
<!-- backwards compatibility -->
<a id="schemav2chartofaccounts"></a>
<a id="schema_V2ChartOfAccounts"></a>
<a id="tocSv2chartofaccounts"></a>
<a id="tocsv2chartofaccounts"></a>

```json
{
  "users": {
    "$userID": {
      ".pattern": "^[0-9]{16}$"
    }
  }
}

```

Chart of account

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|[V2ChartSegment](#schemav2chartsegment)|false|none|Segment within a chart of accounts|

<h2 id="tocS_Runtime">Runtime</h2>
<!-- backwards compatibility -->
<a id="schemaruntime"></a>
<a id="schema_Runtime"></a>
<a id="tocSruntime"></a>
<a id="tocsruntime"></a>

```json
"experimental-interpreter"

```

The numscript runtime used to execute the script. Uses "machine" by default, unless the "--experimental-numscript-interpreter" feature flag is passed.

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|string|false|none|The numscript runtime used to execute the script. Uses "machine" by default, unless the "--experimental-numscript-interpreter" feature flag is passed.|

#### Enumerated Values

|Property|Value|
|---|---|
|*anonymous*|experimental-interpreter|
|*anonymous*|machine|

<h2 id="tocS_V2TransactionTemplate">V2TransactionTemplate</h2>
<!-- backwards compatibility -->
<a id="schemav2transactiontemplate"></a>
<a id="schema_V2TransactionTemplate"></a>
<a id="tocSv2transactiontemplate"></a>
<a id="tocsv2transactiontemplate"></a>

```json
{
  "description": "string",
  "script": "string",
  "runtime": "experimental-interpreter"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|description|string|false|none|none|
|script|string|true|none|none|
|runtime|[Runtime](#schemaruntime)|false|none|The numscript runtime used to execute the script. Uses "machine" by default, unless the "--experimental-numscript-interpreter" feature flag is passed.|

<h2 id="tocS_V2TransactionTemplates">V2TransactionTemplates</h2>
<!-- backwards compatibility -->
<a id="schemav2transactiontemplates"></a>
<a id="schema_V2TransactionTemplates"></a>
<a id="tocSv2transactiontemplates"></a>
<a id="tocsv2transactiontemplates"></a>

```json
{
  "property1": {
    "description": "string",
    "script": "string",
    "runtime": "experimental-interpreter"
  },
  "property2": {
    "description": "string",
    "script": "string",
    "runtime": "experimental-interpreter"
  }
}

```

Transaction templates

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|**additionalProperties**|[V2TransactionTemplate](#schemav2transactiontemplate)|false|none|none|

<h2 id="tocS_V2SchemaData">V2SchemaData</h2>
<!-- backwards compatibility -->
<a id="schemav2schemadata"></a>
<a id="schema_V2SchemaData"></a>
<a id="tocSv2schemadata"></a>
<a id="tocsv2schemadata"></a>

```json
{
  "chart": {
    "users": {
      "$userID": {
        ".pattern": "^[0-9]{16}$"
      }
    }
  },
  "transactions": {
    "property1": {
      "description": "string",
      "script": "string",
      "runtime": "experimental-interpreter"
    },
    "property2": {
      "description": "string",
      "script": "string",
      "runtime": "experimental-interpreter"
    }
  }
}

```

Schema data structure for ledger schemas

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|chart|[V2ChartOfAccounts](#schemav2chartofaccounts)|true|none|Chart of account|
|transactions|[V2TransactionTemplates](#schemav2transactiontemplates)|true|none|Transaction templates|

<h2 id="tocS_V2Schema">V2Schema</h2>
<!-- backwards compatibility -->
<a id="schemav2schema"></a>
<a id="schema_V2Schema"></a>
<a id="tocSv2schema"></a>
<a id="tocsv2schema"></a>

```json
{
  "version": "v1.0.0",
  "createdAt": "2023-01-01T00:00:00Z",
  "chart": {
    "users": {
      "$userID": {
        ".pattern": "^[0-9]{16}$"
      }
    }
  },
  "transactions": {
    "property1": {
      "description": "string",
      "script": "string",
      "runtime": "experimental-interpreter"
    },
    "property2": {
      "description": "string",
      "script": "string",
      "runtime": "experimental-interpreter"
    }
  }
}

```

Complete schema structure with metadata

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» version|string|true|none|Schema version|
|» createdAt|string(date-time)|true|none|Schema creation timestamp|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2SchemaData](#schemav2schemadata)|false|none|Schema data structure for ledger schemas|

<h2 id="tocS_V2SchemaResponse">V2SchemaResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2schemaresponse"></a>
<a id="schema_V2SchemaResponse"></a>
<a id="tocSv2schemaresponse"></a>
<a id="tocsv2schemaresponse"></a>

```json
{
  "data": {
    "version": "v1.0.0",
    "createdAt": "2023-01-01T00:00:00Z",
    "chart": {
      "users": {
        "$userID": {
          ".pattern": "^[0-9]{16}$"
        }
      }
    },
    "transactions": {
      "property1": {
        "description": "string",
        "script": "string",
        "runtime": "experimental-interpreter"
      },
      "property2": {
        "description": "string",
        "script": "string",
        "runtime": "experimental-interpreter"
      }
    }
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Schema](#schemav2schema)|true|none|Complete schema structure with metadata|

<h2 id="tocS_V2SchemasCursorResponse">V2SchemasCursorResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2schemascursorresponse"></a>
<a id="schema_V2SchemasCursorResponse"></a>
<a id="tocSv2schemascursorresponse"></a>
<a id="tocsv2schemascursorresponse"></a>

```json
{
  "cursor": {
    "data": [
      {
        "version": "v1.0.0",
        "createdAt": "2023-01-01T00:00:00Z",
        "chart": {
          "users": {
            "$userID": {
              ".pattern": "^[0-9]{16}$"
            }
          }
        },
        "transactions": {
          "property1": {
            "description": "string",
            "script": "string",
            "runtime": "experimental-interpreter"
          },
          "property2": {
            "description": "string",
            "script": "string",
            "runtime": "experimental-interpreter"
          }
        }
      }
    ],
    "hasMore": true,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "pageSize": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|[V2SchemasCursor](#schemav2schemascursor)|true|none|none|

<h2 id="tocS_V2SchemasCursor">V2SchemasCursor</h2>
<!-- backwards compatibility -->
<a id="schemav2schemascursor"></a>
<a id="schema_V2SchemasCursor"></a>
<a id="tocSv2schemascursor"></a>
<a id="tocsv2schemascursor"></a>

```json
{
  "data": [
    {
      "version": "v1.0.0",
      "createdAt": "2023-01-01T00:00:00Z",
      "chart": {
        "users": {
          "$userID": {
            ".pattern": "^[0-9]{16}$"
          }
        }
      },
      "transactions": {
        "property1": {
          "description": "string",
          "script": "string",
          "runtime": "experimental-interpreter"
        },
        "property2": {
          "description": "string",
          "script": "string",
          "runtime": "experimental-interpreter"
        }
      }
    }
  ],
  "hasMore": true,
  "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
  "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
  "pageSize": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[[V2Schema](#schemav2schema)]|true|none|[Complete schema structure with metadata]|
|hasMore|boolean|true|none|none|
|previous|string|false|none|none|
|next|string|false|none|none|
|pageSize|integer|true|none|none|

<h2 id="tocS_V2CreateLedgerRequest">V2CreateLedgerRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createledgerrequest"></a>
<a id="schema_V2CreateLedgerRequest"></a>
<a id="tocSv2createledgerrequest"></a>
<a id="tocsv2createledgerrequest"></a>

```json
{
  "bucket": "string",
  "metadata": {
    "admin": "true"
  },
  "features": {
    "property1": "string",
    "property2": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|bucket|string|false|none|none|
|metadata|[V2Metadata](#schemav2metadata)|false|none|none|
|features|object|false|none|none|
|» **additionalProperties**|string|false|none|none|

<h2 id="tocS_V2Ledger">V2Ledger</h2>
<!-- backwards compatibility -->
<a id="schemav2ledger"></a>
<a id="schema_V2Ledger"></a>
<a id="tocSv2ledger"></a>
<a id="tocsv2ledger"></a>

```json
{
  "name": "string",
  "addedAt": "2019-08-24T14:15:22Z",
  "bucket": "string",
  "deletedAt": "2019-08-24T14:15:22Z",
  "metadata": {
    "admin": "true"
  },
  "features": {
    "property1": "string",
    "property2": "string"
  },
  "id": 0
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|name|string|true|none|none|
|addedAt|string(date-time)|true|none|none|
|bucket|string|true|none|none|
|deletedAt|string(date-time)¦null|false|none|none|
|metadata|[V2Metadata](#schemav2metadata)|false|none|none|
|features|object|false|none|none|
|» **additionalProperties**|string|false|none|none|
|id|integer|false|none|none|

<h2 id="tocS_V2LedgerListResponse">V2LedgerListResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2ledgerlistresponse"></a>
<a id="schema_V2LedgerListResponse"></a>
<a id="tocSv2ledgerlistresponse"></a>
<a id="tocsv2ledgerlistresponse"></a>

```json
{
  "cursor": {
    "pageSize": 15,
    "hasMore": false,
    "previous": "YXVsdCBhbmQgYSBtYXhpbXVtIG1heF9yZXN1bHRzLol=",
    "next": "aW0gdmVuaWFtLCBxdWlzIG5vc3RydWQ=",
    "data": [
      {
        "name": "string",
        "addedAt": "2019-08-24T14:15:22Z",
        "bucket": "string",
        "deletedAt": "2019-08-24T14:15:22Z",
        "metadata": {
          "admin": "true"
        },
        "features": {
          "property1": "string",
          "property2": "string"
        },
        "id": 0
      }
    ]
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|cursor|object|true|none|none|
|» pageSize|integer(int64)|true|none|none|
|» hasMore|boolean|true|none|none|
|» previous|string|false|none|none|
|» next|string|false|none|none|
|» data|[[V2Ledger](#schemav2ledger)]|true|none|none|

<h2 id="tocS_V2UpdateLedgerMetadataRequest">V2UpdateLedgerMetadataRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2updateledgermetadatarequest"></a>
<a id="schema_V2UpdateLedgerMetadataRequest"></a>
<a id="tocSv2updateledgermetadatarequest"></a>
<a id="tocsv2updateledgermetadatarequest"></a>

```json
{
  "admin": "true"
}

```

### Properties

*None*

<h2 id="tocS_V2GetLedgerResponse">V2GetLedgerResponse</h2>
<!-- backwards compatibility -->
<a id="schemav2getledgerresponse"></a>
<a id="schema_V2GetLedgerResponse"></a>
<a id="tocSv2getledgerresponse"></a>
<a id="tocsv2getledgerresponse"></a>

```json
{
  "data": {
    "name": "string",
    "addedAt": "2019-08-24T14:15:22Z",
    "bucket": "string",
    "deletedAt": "2019-08-24T14:15:22Z",
    "metadata": {
      "admin": "true"
    },
    "features": {
      "property1": "string",
      "property2": "string"
    },
    "id": 0
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|data|[V2Ledger](#schemav2ledger)|true|none|none|

<h2 id="tocS_V2ImportLogsRequest">V2ImportLogsRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2importlogsrequest"></a>
<a id="schema_V2ImportLogsRequest"></a>
<a id="tocSv2importlogsrequest"></a>
<a id="tocsv2importlogsrequest"></a>

```json
{
  "file": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|file|string(binary)|true|none|none|

<h2 id="tocS_V2RevertTransactionRequest">V2RevertTransactionRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2reverttransactionrequest"></a>
<a id="schema_V2RevertTransactionRequest"></a>
<a id="tocSv2reverttransactionrequest"></a>
<a id="tocsv2reverttransactionrequest"></a>

```json
{
  "metadata": {
    "property1": "string",
    "property2": "string"
  }
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|metadata|object|false|none|none|
|» **additionalProperties**|string|false|none|none|

<h2 id="tocS_V2CreatePipelineRequest">V2CreatePipelineRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createpipelinerequest"></a>
<a id="schema_V2CreatePipelineRequest"></a>
<a id="tocSv2createpipelinerequest"></a>
<a id="tocsv2createpipelinerequest"></a>

```json
{
  "exporterID": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|exporterID|string|true|none|none|

<h2 id="tocS_V2CreateExporterRequest">V2CreateExporterRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2createexporterrequest"></a>
<a id="schema_V2CreateExporterRequest"></a>
<a id="tocSv2createexporterrequest"></a>
<a id="tocsv2createexporterrequest"></a>

```json
{
  "driver": "string",
  "config": {}
}

```

### Properties

*None*

<h2 id="tocS_V2UpdateExporterRequest">V2UpdateExporterRequest</h2>
<!-- backwards compatibility -->
<a id="schemav2updateexporterrequest"></a>
<a id="schema_V2UpdateExporterRequest"></a>
<a id="tocSv2updateexporterrequest"></a>
<a id="tocsv2updateexporterrequest"></a>

```json
{
  "driver": "string",
  "config": {}
}

```

### Properties

*None*

<h2 id="tocS_V2PipelineConfiguration">V2PipelineConfiguration</h2>
<!-- backwards compatibility -->
<a id="schemav2pipelineconfiguration"></a>
<a id="schema_V2PipelineConfiguration"></a>
<a id="tocSv2pipelineconfiguration"></a>
<a id="tocsv2pipelineconfiguration"></a>

```json
{
  "ledger": "string",
  "exporterID": "string"
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|ledger|string|true|none|none|
|exporterID|string|true|none|none|

<h2 id="tocS_V2ExporterConfiguration">V2ExporterConfiguration</h2>
<!-- backwards compatibility -->
<a id="schemav2exporterconfiguration"></a>
<a id="schema_V2ExporterConfiguration"></a>
<a id="tocSv2exporterconfiguration"></a>
<a id="tocsv2exporterconfiguration"></a>

```json
{
  "driver": "string",
  "config": {}
}

```

### Properties

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|driver|string|true|none|none|
|config|object|true|none|none|

<h2 id="tocS_V2Exporter">V2Exporter</h2>
<!-- backwards compatibility -->
<a id="schemav2exporter"></a>
<a id="schema_V2Exporter"></a>
<a id="tocSv2exporter"></a>
<a id="tocsv2exporter"></a>

```json
{
  "driver": "string",
  "config": {},
  "id": "string",
  "createdAt": "2019-08-24T14:15:22Z"
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2ExporterConfiguration](#schemav2exporterconfiguration)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» id|string|true|none|none|
|» createdAt|string(date-time)|true|none|none|

<h2 id="tocS_V2Pipeline">V2Pipeline</h2>
<!-- backwards compatibility -->
<a id="schemav2pipeline"></a>
<a id="schema_V2Pipeline"></a>
<a id="tocSv2pipeline"></a>
<a id="tocsv2pipeline"></a>

```json
{
  "id": "string",
  "createdAt": "2019-08-24T14:15:22Z",
  "lastLogID": 0,
  "enabled": true
}

```

### Properties

allOf

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|[V2PipelineConfiguration](#schemav2pipelineconfiguration)|false|none|none|

and

|Name|Type|Required|Restrictions|Description|
|---|---|---|---|---|
|*anonymous*|object|false|none|none|
|» id|string|true|none|none|
|» createdAt|string(date-time)|true|none|none|
|» lastLogID|integer|false|none|none|
|» enabled|boolean|false|none|none|

