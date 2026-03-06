# waveQl
a smart query builder




◤◤◤ waveQl – The SQL Builder That Speaks Your Language

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

**waveQl** is not just another boring query builder. It thinks with you.  
You tell it which fields you have, and it builds a perfect SQL query – including **joins, pagination, sorting** and above all: **operators directly inside the value**.

> 👉 `">10"`, `"!NULL"`, `"~text~"`, `"10><20"` – that's not rocket science, that's waveQl.





◤◤◤ Why waveQl?

Because filtering should be fun.  
Look at this:

```php
$input = [
        'age'   => '>18',           // age > 18
        'name'  => '~müller~',      // name LIKE '%müller%'
        '~or~'  => [
                    'city'   => '!EMPTY', 
                    'points' => '>1000'
                    ]
        ];
```

No where() chains, no callbacks – just clean, readable values.
waveQl automatically parses the operators and builds the correct SQL condition.

And that's just the beginning.

Features – what awaits you

- Operator parsing – <, >, <=, >=, !, ~like~, !NULL, BLANK, EMPTY and even ranges like 10><20 or 5><=15.
- Magic keys – BLANK, !BLANK, EMPTY, !EMPTY – automatically adapt to the field type (string, number, date).
- Automatic date fields – from a date field you get fieldYEAR, fieldMONTH, fieldDAY, fieldTIME, fieldUTS – without extra code.
- Joins – LEFT, RIGHT, INNER, CROSS, STRAIGHT – all there.
- Pagination & sorting – via pageNumber, pageSize and sort (e.g. '>name,<id').
- Fulltext search – with searchString and searchTarget.
- Custom SQL – security‑checked, with placeholder replacement.
- Prepared statements – optional but highly recommended.
- Flat OR groups – simply '~or~' => ['field' => 'value', ...].





◤◤◤ Installation

Via Composer:
bash

composer require e2see/waveql

No other dependencies – just PHP ≥7.4 and a working mysqli object.



◤◤◤ Quick Example

Imagine you have a users table with related orders.
This is how easy it is with waveQl:

```php

// waveQl – Because filtering should be intuitive

require_once 'class.e2.waveQl.php';
use e2\waveQl;

$db = new mysqli('localhost', 'root', '', 'mydb');

$tableInfo = [
    'tableName' => 'users',
    'tableKey'  => 'u',
    'joinList'  => [
        [
            'type'          => 'LEFT',
            'tableName'     => 'orders',
            'tableKey'      => 'o',
            'connectColumn' => 'user_id',
            'connectWith'   => 'u.id'
        ]
    ]
];

$fieldDefinitions = [
    'id'        => ['rowName' => 'u.id',          'type' => 'integer'],
    'firstname' => ['rowName' => 'u.firstname',   'type' => 'string'],
    'lastname'  => ['rowName' => 'u.lastname',    'type' => 'string'],
    'city'      => ['rowName' => 'u.city',        'type' => 'string'],
    'age'       => ['rowName' => 'u.age',         'type' => 'integer'],
    'status'    => ['rowName' => 'u.status',      'type' => 'string'],
    'total'     => ['rowName' => 'o.amount',      'type' => 'float']
];

```

That was the one-time setup – now the real filter fun begins!

```php
$input = [
    'firstname'  => '~Anna~',        // firstname contains "Anna"
    'lastname'   => 'Schmidt',       // lastname exactly "Schmidt"
    'total'      => '500><=1000',    // total between 500 (exclusive) and 1000 (inclusive)
    'city'       => '!BLANK',        // city is not blank (i.e. != '')
    '~or~'       => [                // either status 'active' OR age > 30
        'status' => 'active',
        'age'    => '>30'
    ],
    '~filter~'  => [
        // Arrow up = descending (highest first), arrow down = ascending (A→Z)
        'sort'         => '>total,<lastname',
        'pageNumber'   => 2,
        'pageSize'     => 25,
        'searchString' => 'Müller',                // fulltext search for "Müller"
        'searchTarget' => 'firstname,lastname'     // in firstname and lastname
    ]
];

$builder = new waveQl($db, $tableInfo, $fieldDefinitions, $input, ['prepared' => false]);
echo $builder->getQuery();

```

The resulting SQL – clean and powerful:

```sql

SELECT
    u.id              AS id,
    u.firstname       AS firstname,
    u.lastname        AS lastname,
    u.city            AS city,
    u.age             AS age,
    u.status          AS status,
    o.amount          AS total
FROM 
    users u
LEFT JOIN 
    orders o ON (o.user_id = u.id)
WHERE 1
    AND u.firstname   LIKE '%Anna%'
    AND u.lastname    = 'Schmidt'
    AND o.amount      > 500
    AND o.amount      <= 1000
    AND u.city        != ''
    AND (
        u.status      = 'active'
        OR u.age      > 30
    )
    AND (
        u.firstname   LIKE '%Müller%'
        OR u.lastname LIKE '%Müller%'
    )
ORDER BY 
    total DESC, lastname ASC
LIMIT 25, 25

```
Done. No manual WHERE fiddling, no mistakes with forgotten parentheses.




◤◤◤ More

The complete documentation with all operators, magic keys, and configuration options is in the class DocBlock – take a look at the source.
Everything is explained there: structure of fieldDefinitions, tableInfo, inputValues, all operators, examples.



◤◤◤ Testing & Contributing

Got an idea, found a bug, or just want to say thanks?
Open an issue or a pull request – we appreciate any feedback.



◤◤◤ License

waveQl is released under the MIT license.
You are free to use, modify, and distribute it – even in commercial projects. A little shoutout would be nice, but it's not required.

Try it now and fall in love with filtering! 💙
