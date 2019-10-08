### Book Inventory
##### Description
> This end-point books a number of inventory for a time period.
##### Method: POST
##### URL: http://service-domain/imsservice/api/bookings
#####POST Body:
```sh
{
    targeting_channel:<ucdocument>,
    ranges: [ {
        sd:<start-date date in yyyy-mm-dd>,
        ed:<end-date date in yyyy-mm-dd>,
        sh:<start-hour in hh>,
        eh:<end-hour in hh>} …]
    price: <CPC price> numeric format.
    advId: advertiser id 
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
	"advId": 24512,
	"price":0,
	"days": [{
		"st": "2018-11-07",
		"ed": "2018-11-10",
		"sh": 0,
		"eh": 23
	}],
	"requestCount": 1000
}
```
##### Response:
```sh
{
    "result": {
        "bk_id": "56a1389a-ef6c-45b9-ae5f-936caf9e70de",
        "total_booked": 1000
    }
}
```

<br />
<br />
<br />

### Delete Booking
##### Description
> This end-point deletes a booking.
##### Method: DELETE
##### URL: http://service-domain/imsservice/api/bookings/<bk_id>

<br />
<br />
<br />

### Retrieve Bookings by Advertiser ID
##### Description
> This end-point returns all the bookings that belong to an advertiser.
##### Method: GET
##### URL: http://service-domain/imsservice/api/<advertiser_id>/bookings
##### Response Example:
```sh
{
    "result": [
        {
            "days": [
                "20181107",
                "20181108",
                "20181109",
                "20181110"
            ],
            "bk_id": "45111377-29eb-49e4-a999-de437b66cd60",
            "adv_id": "24512",
            "price": 0.0,
            "amount": 1000,
            "query": {
                "r": null,
                "g": [
                    "g_m"
                ],
                "a": null,
                "t": null,
                "si": null,
                "m": null,
                "aus": null,
                "ais": null,
                "pdas": null,
                "exclude_pdas": null,
                "apps": null,
                "exclude_apps": null,
                "dms": [
                    "rneal00"
                ],
                "pm": "NONE",
                "dpc": null,
                "ipl": null
            },
            "del": false
        }
    ]
}
```

<br />
<br />
<br />

### Retrieve One Booking by BookingID
##### Description
> This end-point returns one specific booking.
##### Method: GET
##### URL: http://service-domain/imsservice/api/bookings/<bk_id>
##### Response Example:
```sh
{
    "result": {
        "days": [
            "20181107",
            "20181108",
            "20181109",
            "20181110"
        ],
        "bk_id": "45111377-29eb-49e4-a999-de437b66cd60",
        "adv_id": "24512",
        "price": 0.0,
        "amount": 1000,
        "query": {
            "r": null,
            "g": [
                "g_m"
            ],
            "a": null,
            "t": null,
            "si": null,
            "m": null,
            "aus": null,
            "ais": null,
            "pdas": null,
            "exclude_pdas": null,
            "apps": null,
            "exclude_apps": null,
            "dms": [
                "rneal00"
            ],
            "pm": "NONE",
            "dpc": null,
            "ipl": null
        },
        "del": false
    }
}
```