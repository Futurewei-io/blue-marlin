### Chart
##### Description
> This end-point returns inventory for the next day in hours format for a targeting channel.
##### Method: POST
##### URL: http://service-domain/imsservice/api/chart
#####POST Body:
```sh
{
    targeting_channel:<ucdocument>,
    price: <CPC price> numeric format.
}

The following attributes are used in targeting channel:
1)	pm: Price Model – CPM/CPC/CPD/CPT
2)	ai: Ad slot - list
3)	ipl: Geo-location – list of city code
4)	r: Resident-location – list of city code
5)	g: Gender - list
6)	a: Age – list
-	0 - (0,18)
-	1 - [18,24]
-	2 - (24,34]
-	3 - (34,44]
-	4 - (44,54]
-	5 - (55,+)
7)	aus: App usage – list of {category?_appusage_state}, state is one of following
a.	active_30days
b.	activated
c.	installed_not_activated
d.	not_instatlled
8)	ais: App interest – list
9)	t: Connection type - list
10)	pdas: Pre-defined audiences – list
11)	exclude_pdas: Excluded pre-defined audiences - list
12)	dms: Device model – list
13)	dpc: Device price category range of [0,4] - list
-	(0-1500] – 0_1500
-	(1500-2500] – 1500_2500
-	(2500-3500] – 2500_3500
-	(3500-4500] – 3500_4500
-	(4500-+) – 4500


Excample:
{
    "targetingChannel": {
        "g":["g_m"],
        "dms":["rneal00"]       
    },
    "price":0
}
```
##### Response Example:
```sh
{
    "result": {
        "date": "2019-10-01",
        "hours": [
            {
                "hs": [
                    0.0,
                    934.0,
                    5139.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 934,
                    "h": "h1"
                },
                "h2": {
                    "t": 5139,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 6073
            },
            {
                "hs": [
                    0.0,
                    6333.0,
                    8150.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 6333,
                    "h": "h1"
                },
                "h2": {
                    "t": 8150,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 14483
            },
            {
                "hs": [
                    0.0,
                    3789.0,
                    6021.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 3789,
                    "h": "h1"
                },
                "h2": {
                    "t": 6021,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 9810
            },
            {
                "hs": [
                    0.0,
                    5710.0,
                    1453.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 5710,
                    "h": "h1"
                },
                "h2": {
                    "t": 1453,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 7163
            },
            {
                "hs": [
                    0.0,
                    9395.0,
                    3322.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 9395,
                    "h": "h1"
                },
                "h2": {
                    "t": 3322,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 12717
            },
            {
                "hs": [
                    0.0,
                    2180.0,
                    3634.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 2180,
                    "h": "h1"
                },
                "h2": {
                    "t": 3634,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 5814
            },
            {
                "hs": [
                    0.0,
                    7423.0,
                    2440.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 7423,
                    "h": "h1"
                },
                "h2": {
                    "t": 2440,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 9863
            },
            {
                "hs": [
                    0.0,
                    3789.0,
                    5554.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 3789,
                    "h": "h1"
                },
                "h2": {
                    "t": 5554,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 9343
            },
            {
                "hs": [
                    0.0,
                    6437.0,
                    7163.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 6437,
                    "h": "h1"
                },
                "h2": {
                    "t": 7163,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 13600
            },
            {
                "hs": [
                    0.0,
                    5866.0,
                    2440.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 5866,
                    "h": "h1"
                },
                "h2": {
                    "t": 2440,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 8306
            },
            {
                "hs": [
                    0.0,
                    5450.0,
                    6229.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 5450,
                    "h": "h1"
                },
                "h2": {
                    "t": 6229,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 11679
            },
            {
                "hs": [
                    0.0,
                    3945.0,
                    8253.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 3945,
                    "h": "h1"
                },
                "h2": {
                    "t": 8253,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 12198
            },
            {
                "hs": [
                    0.0,
                    1661.0,
                    2232.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 1661,
                    "h": "h1"
                },
                "h2": {
                    "t": 2232,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 3893
            },
            {
                "hs": [
                    0.0,
                    4879.0,
                    7942.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 4879,
                    "h": "h1"
                },
                "h2": {
                    "t": 7942,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 12821
            },
            {
                "hs": [
                    0.0,
                    7682.0,
                    3218.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 7682,
                    "h": "h1"
                },
                "h2": {
                    "t": 3218,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 10900
            },
            {
                "hs": [
                    0.0,
                    6592.0,
                    9966.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 6592,
                    "h": "h1"
                },
                "h2": {
                    "t": 9966,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 16558
            },
            {
                "hs": [
                    0.0,
                    6281.0,
                    2232.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 6281,
                    "h": "h1"
                },
                "h2": {
                    "t": 2232,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 8513
            },
            {
                "hs": [
                    0.0,
                    3218.0,
                    7267.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 3218,
                    "h": "h1"
                },
                "h2": {
                    "t": 7267,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 10485
            },
            {
                "hs": [
                    0.0,
                    6229.0,
                    5087.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 6229,
                    "h": "h1"
                },
                "h2": {
                    "t": 5087,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 11316
            },
            {
                "hs": [
                    0.0,
                    2336.0,
                    7163.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 2336,
                    "h": "h1"
                },
                "h2": {
                    "t": 7163,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 9499
            },
            {
                "hs": [
                    0.0,
                    2907.0,
                    6748.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 2907,
                    "h": "h1"
                },
                "h2": {
                    "t": 6748,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 9655
            },
            {
                "hs": [
                    0.0,
                    7008.0,
                    7682.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 7008,
                    "h": "h1"
                },
                "h2": {
                    "t": 7682,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 14690
            },
            {
                "hs": [
                    0.0,
                    4724.0,
                    3737.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 4724,
                    "h": "h1"
                },
                "h2": {
                    "t": 3737,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 8461
            },
            {
                "hs": [
                    0.0,
                    4776.0,
                    4049.0,
                    0.0
                ],
                "h0": {
                    "t": 0,
                    "h": "h0"
                },
                "h1": {
                    "t": 4776,
                    "h": "h1"
                },
                "h2": {
                    "t": 4049,
                    "h": "h2"
                },
                "h3": {
                    "t": 0,
                    "h": "h3"
                },
                "h_index": null,
                "total": 8825
            }
        ]
    }
}
```