![Demo](images/logo-s.png)


[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE) a smart query builder


# waveQl – The SQL Builder That Speaks Your Language




**waveQl** is not just another boring query builder. It thinks with you.
You tell it which fields you have, and it builds a perfect SQL query – including **joins, pagination, sorting** and above all: **operators directly inside the value**.

> 👉 `">10"`, `"!NULL"`, `"~text~"`, `"10><20"` – that's not rocket science, that's waveQl.



<br><br>
◤◤◤ Why waveQl?

Because filtering should be fun.
Look at this:

```php
$filters = [
    'FoundedDateYEAR' => '1900><=1950',   // between 1901 and 1950 (inclusive)
    'ContinentName'   => 'Asia',
    '~or~'            => [
                        'Population' => '>60000000',
                        'AreaKm2'    => '>2200000'
                        ]
];
```

No where() chains, no callbacks – just clean, readable values.
waveQl automatically parses the operators and builds the correct SQL condition.

And that's just the beginning.

Features – what awaits you

- Operator parsing – `<`, `>`, `<=`, `>=`, `!`, `~like~`, `!NULL`, `BLANK`, `EMPTY` and even ranges like `10><20` or `5><=15`.
- Magic keys – `BLANK`, `!BLANK`, `EMPTY`, `!EMPTY` – automatically adapt to the field type (string, number, date).
- Automatic date fields – from a date field you get `fieldYEAR`, `fieldMONTH`, `fieldDAY`, `fieldTIME`, `fieldUTS` – without extra code.
- Joins – `LEFT`, `RIGHT`, `INNER`, `CROSS`, `STRAIGHT` – all there.
- Pagination & sorting – via `pageNumber`, `pageSize` and sort (e.g. '>name,<id').
- Fulltext search – with `searchString` and `searchTarget`.
- Custom SQL – security‑checked, with placeholder replacement.
- Prepared statements – optional but highly recommended.
- Flat OR groups – simply `'~or~' => ['field' => 'value', ...]`.



<br><br>
◤◤◤ Quick Example

Imagine you have the following countries and continents tables.
This is how easy it is with waveQl:

```php

// waveQl – Because filtering should be intuitive
require_once 'waveQl.php';


// Table and join configuration
$tableManifest = [
    'tableName' => 'countries',
    'tableKey'  => 'c',
    'joinList'  => [
        [
            'type'          => 'LEFT',
            'tableName'     => 'continents',
            'tableKey'      => 'cnt',
            'connectColumn' => 'id',
            'connectWith'   => 'c.continent_id'
        ]
    ]
];


//  Field definitions
//  logical name    =>               SQL column                   SQL type
$keyManifest = [
    'AreaKm2'       => ['rowName' => 'c.area_km2',      'type' => 'integer'],
    'Capital'       => ['rowName' => 'c.capital',       'type' => 'string'],
    'ContinentId'   => ['rowName' => 'cnt.id',          'type' => 'integer'],
    'ContinentName' => ['rowName' => 'cnt.name',        'type' => 'string'],
    'CountryName'   => ['rowName' => 'c.name',          'type' => 'string'],
    'FoundedDate'   => ['rowName' => 'c.founded_date',  'type' => 'date'],
    'Population'    => ['rowName' => 'c.population',    'type' => 'integer'],
    '~meta~'        => [ // default meta settings (sort, pageSize, searchTarget)
        'sort'         => '>CountryName',
        'pageSize'     => 20,
        'searchTarget' => 'CountryName,Capital,ContinentName'
    ],
];


$db      = new mysqli('localhost', 'root', '', 'mydb');
$wave    = \e2\waveQl::create($db, $tableManifest, $keyManifest);
$builder = $wave->read(); // read-modus (also possible: write for INSERTS)


```

That was the one-time setup – now the real filter fun begins!

```php

$filters = [
    'FoundedDateYEAR' => '1900><=1950',   // between 1901 and 1950 (inclusive)
    'ContinentName'   => 'Asia',
    '~or~'            => [
                        'Population' => '>60000000',
                        'AreaKm2'    => '>2200000'
                        ]
];


// get the final SQL
echo $builder->setValues($filters)->getQuery();


```



The resulting SQL – clean and powerful:

```sql

SELECT
    `c`.`area_km2`           AS AreaKm2,
    `c`.`capital`            AS Capital,
    `cnt`.`id`               AS ContinentId,
    `cnt`.`name`             AS ContinentName,
    `c`.`name`               AS CountryName,
    `c`.`founded_date`       AS FoundedDate,
    DATE(c.founded_date)     AS FoundedDateDATE,
    DAY(c.founded_date)      AS FoundedDateDAY,
    MONTH(c.founded_date)    AS FoundedDateMONTH,
    QUARTER(c.founded_date)  AS FoundedDateQUARTER,
    UNIX_TIMESTAMP(c.founded_date) AS FoundedDateUTS,
    YEAR(c.founded_date)     AS FoundedDateYEAR,
    `c`.`population`         AS Population
FROM
    `countries` `c`
        LEFT JOIN
            `continents` `cnt`
            ON (`cnt`.`id` = `c`.`continent_id`)
WHERE 1
    AND `cnt`.`name`         = 'Asia'
    AND `c`.`founded_year`   > 1900
    AND `c`.`founded_year`   <= 1950
    AND (
            (`c`.`area_km2`  > 2200000)
         OR (`c`.`population` > 60000000)
    )
ORDER BY
    `CountryName`            DESC
LIMIT
    0, 20

```


Done. No manual WHERE fiddling, no mistakes with forgotten parentheses.

<br />

And there you have it – the matching records, complete with those handy **auto‑generated date/time columns**.


```sql

CountryName       | Turkey          | Pakistan        | Indonesia       | India
Population        | 84,300,000      | 220,000,000     | 273,000,000     | 1,380,000,000
AreaKm2           | 783,600         | 881,900         | 1,905,000       | 3,287,000
Capital           | Ankara          | Islamabad       | Jakarta         | New Delhi
FoundedDateDATE   | 1923-10-29      | 1947-08-14      | 1945-08-17      | 1947-08-15
FoundedDateYEAR   | 1923            | 1947            | 1945            | 1947
FoundedDateQUARTER| 4               | 3               | 3               | 3
FoundedDateMONTH  | 10              | 8               | 8               | 8
FoundedDateDAY    | 29              | 14              | 17              | 15
FoundedDateUTS    | NULL            | NULL            | NULL            | NULL
FoundedDate       | 1923-10-29      | 1947-08-14      | 1945-08-17      | 1947-08-15
ContinentName     | Asia            | Asia            | Asia            | Asia

```

Want to give it a spin yourself? The included Playground lets you experiment with all operators, magic keys, and joins live.


<br><br>
◤◤◤ Status of Write Operations

**Note:** Write operations (`INSERT`, `UPDATE`, `DELETE`) are already present in the codebase but are **not yet considered stable**. The API may still change. For production use, only read operations (`SELECT`) are recommended at this time. Write support will be finalized in one of the next releases.


<br><br>
◤◤◤ Installation

**No Composer required** – just copy the waveQl files into your project and include them manually.
If you prefer Composer, you can add the repository to your `composer.json` (the package is not yet published on Packagist).

**Requirements**
- PHP ≥8.1
- A database connection: either a `mysqli` object **or** an object implementing the `waveQlDbInterface` (e.g. a custom PDO wrapper).

No other dependencies – just PHP ≥8.1 and a database connection (mysqli or a custom adapter).




<br><br>
◤◤◤ Playground

The repository includes a **ready‑to‑run Playground**. Just copy the waveQl files into the parent directory and point your browser to `index.php`. You can explore all operators, magic keys, and joins interactively.

(images/playground-screenshot.jpg)


A **database setup script** is included – simply click the *"reset/initialise database"* link to create the demo tables and sample data. The interface lets you apply filters, see the generated SQL, and execute queries in real time. It's the perfect sandbox to experiment with waveQl's features.


<br><br>
◤◤◤ More

The complete documentation with all operators, magic keys, and configuration options is in the class DocBlock – take a look at the source.
Everything is explained there: structure of keyManifest, tableManifest, filters, all operators, examples.



<br><br>
◤◤◤ Testing & Contributing

Got an idea, found a bug, or just want to say thanks?
Open an issue or a pull request – we appreciate any feedback.



<br><br>
◤◤◤ License

waveQl is released under the MIT license.
You are free to use, modify, and distribute it – even in commercial projects. A little shoutout would be nice, but it's not required.

Try it now and fall in love with filtering! 💙
