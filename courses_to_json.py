import argparse
import hashlib
import json
import re
import time
import urllib.parse
from datetime import datetime, timezone
from functools import cache
from itertools import repeat
from multiprocessing import Pool
from pathlib import Path
from typing import Any, Optional

import requests
from tqdm import tqdm

POOL_CONCURRENT_PROCESSES = 16

CACHE_DIR_PATH = Path(".cache_dir")
CACHE_DIR: Optional[Path] = (
    Path(CACHE_DIR_PATH.read_text(encoding="utf-8").strip())
    if CACHE_DIR_PATH.exists()
    else None
)

VERBOSE_LOGGING = False

session = requests.session()

session.proxies = {
    # Use fiddler as proxy
    # "http": "http://127.0.0.1:8888",
    # "https": "http://127.0.0.1:8888",
    # Use tor as proxy
    # "http": "socks5://127.0.0.1:9050",
    # "https": "socks5://127.0.0.1:9050",
}


def send_request_once(query: str):
    cache_file_path = None
    if CACHE_DIR:
        CACHE_DIR.mkdir(parents=True, exist_ok=True)

        cache_name_prefix = re.sub(r"[<>:\"/\\|?*]", "_", query)[:64]
        cache_hash = int.from_bytes(
            hashlib.sha256(query.encode()).digest()[:8], "little"
        )
        cache_file_path = CACHE_DIR / f"{cache_name_prefix}_{cache_hash:x}.json"

        if cache_file_path.exists():
            with cache_file_path.open(encoding="utf-8") as f:
                return json.load(f)

    if VERBOSE_LOGGING:
        print(f"Sending request: {query}")

    url = "https://portalex.technion.ac.il/sap/opu/odata/sap/Z_CM_EV_CDIR_DATA_SRV/$batch?sap-client=700"

    headers = {
        # "Host": "portalex.technion.ac.il",
        # "Connection": "keep-alive",
        # "Content-Length": "955",
        "sec-ch-ua": '"Not/A)Brand";v="8", "Chromium";v="126", "Brave";v="126"',
        "MaxDataServiceVersion": "2.0",
        "Accept-Language": "he",
        "sec-ch-ua-mobile": "?0",
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like"
            " Gecko) Chrome/126.0.0.0 Safari/537.36"
        ),
        "Content-Type": "multipart/mixed;boundary=batch_1d12-afbf-e3c7",
        "Accept": "multipart/mixed",
        "sap-contextid-accept": "header",
        "sap-cancel-on-close": "true",
        "X-Requested-With": "X",
        "DataServiceVersion": "2.0",
        # "SAP-PASSPORT": SAP_PASSPORT,
        "sec-ch-ua-platform": '"Windows"',
        "Sec-GPC": "1",
        "Origin": "https://portalex.technion.ac.il",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-Mode": "cors",
        "Sec-Fetch-Dest": "empty",
        "Referer": "https://portalex.technion.ac.il/ovv/",
        # "Accept-Encoding": "gzip, deflate, br, zstd",
        # "Cookie": SAP_COOKIE,
    }

    data = f"""
--batch_1d12-afbf-e3c7
Content-Type: application/http
Content-Transfer-Encoding: binary

GET {query} HTTP/1.1
sap-cancel-on-close: true
X-Requested-With: X
sap-contextid-accept: header
Accept: application/json
Accept-Language: he
DataServiceVersion: 2.0
MaxDataServiceVersion: 2.0


--batch_1d12-afbf-e3c7--
"""
    data = data.replace("\n", "\r\n")

    response = session.post(url, headers=headers, data=data)
    if response.status_code != 202:
        raise RuntimeError(f"Bad status code: {response.status_code}, expected 202")

    response_chunks = response.text.replace("\r\n", "\n").strip().split("\n\n")
    if len(response_chunks) != 3:
        raise RuntimeError(f"Invalid response: {response_chunks}")

    json_str = response_chunks[2].split("\n", 1)[0]

    if VERBOSE_LOGGING:
        print(f"Got {len(json_str)} bytes")

    result = json.loads(json_str)

    if cache_file_path:
        with cache_file_path.open("w", encoding="utf-8") as f:
            json.dump(result, f, indent=2, ensure_ascii=False)

    return result


def send_request(query: str):
    delay = 5
    while True:
        try:
            return send_request_once(query)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 300)


def sap_date_parse(date_str: str):
    match = re.fullmatch(r"/Date\((\d+)\)/", date_str)
    if not match:
        raise RuntimeError(f"Invalid date: {date_str}")

    return datetime.fromtimestamp(int(match.group(1)) / 1000, timezone.utc)


def to_new_course_number(course):
    match = re.match(r'^9730(\d\d)$', course)
    if match:
        return '970300' + match.group(1)

    match = re.match(r'^(\d\d\d)(\d\d\d)$', course)
    if match:
        return '0' + match.group(1) + '0' + match.group(2)

    return course


def get_last_semesters(semester_count: int):
    params = {
        "sap-client": "700",
        "$select": ",".join(
            [
                "PiqYear",
                "PiqSession",
                "Begda",
                "Endda",
            ]
        ),
        # "$inlinecount": "allpages",
    }
    raw_data = send_request(f"SemesterSet?{urllib.parse.urlencode(params)}")
    raw_results = raw_data["d"]["results"]
    if not raw_results:
        raise RuntimeError("No semesters found")

    results = []
    for result in raw_results:
        year = int(result["PiqYear"])
        semester = int(result["PiqSession"])
        if semester not in [200, 201, 202]:
            continue

        begin_date = sap_date_parse(result["Begda"]).strftime("%Y-%m-%d")
        end_date = sap_date_parse(result["Endda"]).strftime("%Y-%m-%d")

        results.append(
            {
                "year": year,
                "semester": semester,
                "start": begin_date,
                "end": end_date,
            },
        )

    def results_sort_key(result):
        return result["year"], result["semester"]

    return sorted(results, key=results_sort_key, reverse=True)[:semester_count]


def get_sap_course_numbers(year: int, semester: int):
    params = {
        "sap-client": "700",
        "$skip": "0",
        "$top": "10000",
        "$filter": f"Peryr eq '{year}' and Perid eq '{semester}'",
        "$select": ",".join(
            [
                "Otjid",
            ]
        ),
        # "$inlinecount": "allpages",
    }
    raw_data = send_request(f"SmObjectSet?{urllib.parse.urlencode(params)}")
    return [x["Otjid"] for x in raw_data["d"]["results"]]


def get_sap_course(year: int, semester: int, course: str):
    params = {
        "sap-client": "700",
        # "$skip": "0",
        # "$top": "50",
        # "$orderby": "Short asc",
        "$filter": (
            f"Peryr eq '{year}' and Perid eq '{semester}' and Otjid eq '{course}'"
        ),
        "$select": ",".join(
            [
                "Otjid",
                "Points",
                "Name",
                "StudyContentDescription",
                "OrgText",
                "ZzAcademicLevel",  # Without this, ZzAcademicLevelText is wrong
                "ZzAcademicLevelText",
                "ZzSemesterNote",
                "Responsible",
                "Exams",
                "SmRelations",
                "SmPrereq",
            ]
        ),
        "$expand": ",".join(
            [
                "Responsible",
                "Exams",
                "SmRelations",
                "SmPrereq",
            ]
        ),
        # "$inlinecount": "allpages",
    }
    raw_data = send_request(f"SmObjectSet?{urllib.parse.urlencode(params)}")
    results = raw_data["d"]["results"]
    if len(results) != 1:
        raise RuntimeError(f"Invalid results for {course}: {results}")

    return results[0]


@cache
def get_building_name(year: int, semester: int, room_id: str):
    params = {
        "sap-client": "700",
        "$select": ",".join(
            [
                "Building",
            ]
        ),
    }
    raw_data = send_request(
        f"GObjectSet(Otjid='{urllib.parse.quote(room_id)}',Peryr='{year}',Perid='{semester}')?{urllib.parse.urlencode(params)}"
    )
    building = raw_data["d"]["Building"]
    if not building:
        raise RuntimeError(f"Invalid building for room: {room_id}")

    building = re.sub(r"\s+", " ", building.strip())

    mapping = {
        "בנין אולמן": "אולמן",
        "בנין בורוביץ הנדסה אזרחית": "בורוביץ הנדסה אזרחית",
        "בנין דן קהאן": "דן קהאן",
        "בנין הנ' אוירונאוטית": "הנ' אוירונאוטית",
        "בנין זיסאפל": "זיסאפל",
        "בנין להנדסת חמרים": "הנדסת חמרים",
        "בנין ליידי דייוס": "ליידי דייוס",
        "בנין למדעי המחשב": "מדעי המחשב",
        "בנין ע'ש אמדו": "אמדו",
        "בנין ע'ש טאוב": "טאוב",
        "בנין ע'ש סגו": "סגו",
        "בנין פישבך": "פישבך",
        "בנין פקולטה לרפואה": "פקולטה לרפואה",
        "בניין ננו-אלקטרוניקה": "ננו-אלקטרוניקה",
        "בניין ספורט": "ספורט",
    }

    for prefix_from, prefix_to in mapping.items():
        if building.startswith(prefix_from):
            return prefix_to + building[len(prefix_from) :]

    return building


def get_room_info(year: int, semester: int, event_schedule_id: str):
    params = {
        "sap-client": "700",
        "$filter": (
            f"Otjid eq '{event_schedule_id}' and Peryr eq '{year}' and Perid eq"
            f" '{semester}'"
        ),
        "$expand": ",".join(
            [
                "Rooms",
                # TODO
                # "Persons"
            ]
        ),
    }
    raw_data = send_request(f"EventScheduleSet?{urllib.parse.urlencode(params)}")
    results = raw_data["d"]["results"]

    rooms_by_time = {}

    for result in results:
        # TODO
        # if result["Persons"]["results"] != []:
        #     exit(result)

        date_raw = result["Evdat"]
        begin_raw = result["Beguz"]
        end_raw = result["Enduz"]
        if not date_raw or not begin_raw or not end_raw:
            raise RuntimeError(f"Missing date/time for {event_schedule_id}")

        date = sap_date_parse(date_raw)
        weekday = (date.weekday() + 1) % 7

        if match := re.fullmatch(r"PT(\d\d)H(\d\d)M00S", begin_raw):
            begin_time = f"{match.group(1)}:{match.group(2)}"
        else:
            raise RuntimeError(
                f"Invalid begin time for {event_schedule_id}: {begin_raw}"
            )

        if match := re.fullmatch(r"PT(\d\d)H(\d\d)M00S", end_raw):
            end_time = f"{match.group(1)}:{match.group(2)}"
        else:
            raise RuntimeError(f"Invalid end time for {event_schedule_id}: {end_raw}")

        weekday_and_time = (weekday, begin_time, end_time)

        rooms = result["Rooms"]["results"]

        buildings = set()
        room_numbers = set()
        for room in rooms:
            room_id = room["Otjid"]
            room_name = room["Name"]

            if match := re.fullmatch(r"(\d\d\d)-(\d\d\d\d)", room_name):
                building = get_building_name(year, semester, room_id)
                room_number = int(match.group(2))
                buildings.add(building)
                room_numbers.add(room_number)
            else:
                raise RuntimeError(
                    f"Invalid room name for {event_schedule_id}: {room_name}"
                )

        if len(buildings) != 1:
            continue

        building = buildings.pop()
        room_number = room_numbers.pop() if len(room_numbers) == 1 else 0

        building_and_room = (building, room_number)

        rooms_by_time[weekday_and_time] = building_and_room

    return rooms_by_time


def get_course_schedule(year: int, semester: int, course_number: str):
    params = {
        "sap-client": "700",
        "$expand": ",".join(
            [
                "EObjectSet",
                "EObjectSet/Persons",
            ]
        ),
    }
    raw_data = send_request(
        f"SmObjectSet(Otjid='SM{course_number}',Peryr='{year}',Perid='{semester}',ZzCgOtjid='',ZzPoVersion='',ZzScOtjid='')/SeObjectSet?{urllib.parse.urlencode(params)}"
    )
    raw_schedule_results = raw_data["d"]["results"]
    if len(raw_schedule_results) == 0:
        return []

    result = []

    def raw_schedule_sort_key(raw_schedule):
        # Sort by group id in ascending order, but place 0 groups at the end.
        group_id = int(raw_schedule["ZzSeSeqnr"])
        return group_id == 0, group_id

    for raw_schedule in sorted(raw_schedule_results, key=raw_schedule_sort_key):
        group_id = int(raw_schedule["ZzSeSeqnr"])

        raw_schedule_items = raw_schedule["EObjectSet"]["results"]
        for raw_schedule_item in raw_schedule_items:
            category = raw_schedule_item["CategoryText"]
            is_sport_course = re.fullmatch(r'03940[89]\d\d', course_number) is not None
            if is_sport_course:
                if category not in ["ספורט", "נבחרת ספורט"]:
                    raise RuntimeError(f"Invalid category: {category}")
                category = raw_schedule_item["Name"]
                # Sometimes the item name is generic and the schedule group item
                # is more descriptive.
                if (
                    category.startswith("ספורט חינוך גופני - ")
                    or category == "ספורט נבחרות ספורט"
                ) and raw_schedule["Name"]:
                    category = raw_schedule["Name"]
            # Temporary special case.
            elif (
                course_number == "00950219"
                and category == ""
                and raw_schedule_item["Name"].startswith("תרגיל")
            ):
                category = "תרגול"
            elif category not in ["הרצאה", "תרגול", "מעבדה", "פרויקט", "סמינר"]:
                raise RuntimeError(f"Invalid category: {category}")

            building = ""
            room = 0
            building_room_dict = None
            building_and_room = raw_schedule_item["RoomText"]
            if building_and_room:
                if match := re.fullmatch(r"(\d\d\d)-(\d\d\d\d)", building_and_room):
                    building = get_building_name(
                        year, semester, raw_schedule_item["RoomId"]
                    )
                    room = int(match.group(2))
                elif building_and_room == "ראה פרטים":
                    building_room_dict = get_room_info(
                        year, semester, raw_schedule_item["Otjid"]
                    )
                else:
                    raise RuntimeError(
                        f"Invalid building and room: {building_and_room}"
                    )

            staff = ""
            for person in raw_schedule_item["Persons"]["results"]:
                title = person["Title"].strip()
                if title and title != "-":
                    staff += f"{title} "
                staff += f"{person['FirstName']} {person['LastName']}\n"
            staff = staff.rstrip("\n")

            event_id = raw_schedule_item["Otjid"]

            date_and_time_list = raw_schedule_item["ScheduleSummary"]
            if date_and_time_list != raw_schedule_item["ScheduleText"]:
                raise RuntimeError(
                    f"Date and time mismatch: {date_and_time_list} !="
                    f" {raw_schedule_item['ScheduleText']}"
                )

            if not date_and_time_list:
                continue

            if date_and_time_list == "לֹא סָדִיר":
                if year >= 2024:
                    print(
                        f"Warning: [{year}/{semester}/{course_number}] Unsupported date"
                        f" and time: {date_and_time_list}"
                    )
                continue

            # Skip specific dates like:
            # "27.05.: 10:00-12:00".
            # "02.02., 03.02., 04.02., בהתאמה 08:00-17:00"
            if re.fullmatch(
                r"\d\d\.\d\d\.: \d\d:\d\d-\d\d:\d\d", date_and_time_list
            ) or re.fullmatch(
                r"(\d\d\.\d\d\., )+בהתאמה \d\d:\d\d-\d\d:\d\d", date_and_time_list
            ):
                continue

            date_and_time_list = re.sub(r"^מ \d\d\.\d\d\., ", "", date_and_time_list)
            date_and_time_list = re.sub(r"^עד \d\d\.\d\d\., ", "", date_and_time_list)
            date_and_time_list = re.sub(
                r"^\d\d\.\d\d\. עד \d\d\.\d\d\., ", "", date_and_time_list
            )
            date_and_time_list = re.sub(r", יוצא מן הכלל: .*$", "", date_and_time_list)
            date_and_time_list = re.sub(r", הכל \d+ ימים$", "", date_and_time_list)
            date_and_time_list = [x.strip() for x in date_and_time_list.split(",")]
            for date_and_time in date_and_time_list:
                match = re.fullmatch(
                    r"(?:יום|יוֹם) (רִאשׁוֹ|שני|שלישי|רביעי|חמישי|שישי)"
                    r" (\d\d:\d\d)\s*-\s*(\d\d:\d\d)",
                    date_and_time,
                )
                if not match:
                    raise RuntimeError(f"Invalid date and time: {date_and_time}")

                day = match.group(1)
                day = "ראשון" if day == "רִאשׁוֹ" else day
                time_begin = match.group(2)
                time_end = match.group(3)
                time = f"{time_begin} - {time_end}"

                if building_room_dict:
                    days = [
                        "ראשון",
                        "שני",
                        "שלישי",
                        "רביעי",
                        "חמישי",
                        "שישי",
                    ]
                    weekday_and_time = (days.index(day), time_begin, time_end)
                    building, room = building_room_dict.get(weekday_and_time, ("", 0))

                result_item = {
                    "קבוצה": group_id,
                    "סוג": category,
                    "יום": day,
                    "שעה": time,
                    "בניין": building,
                    "חדר": room,
                    "מרצה/מתרגל": staff,
                    "מס.": event_id,
                }

                if result_item not in result:
                    result.append(result_item)
                else:
                    print(
                        f"Warning: [{year}/{semester}/{course_number}] Duplicate event:"
                        f" {result_item}"
                    )

    # Make ids prettier by deriving them from groups.
    event_id_to_group = {}
    for event in result:
        groups = event_id_to_group.setdefault(event["מס."], [])
        if event["קבוצה"] not in groups:
            groups.append(event["קבוצה"])

    assigned_ids = {}
    new_ids_events = {}
    for event in result:
        old_id = event["מס."]

        if old_id in assigned_ids:
            event["מס."] = assigned_ids[old_id]
            continue

        if len(event_id_to_group[old_id]) == 1:
            new_id = event_id_to_group[old_id][0]
            fallback_new_id = None
        else:
            new_id = (event["קבוצה"] // 10) * 10
            fallback_new_id = event_id_to_group[old_id][0]

        while new_id in assigned_ids.values() and not (
            new_ids_events[new_id][0]["קבוצה"] == event["קבוצה"]
            and all(x["סוג"] != event["סוג"] for x in new_ids_events[new_id])
        ):
            if fallback_new_id is not None:
                new_id = fallback_new_id
                fallback_new_id = None
            else:
                print(
                    f"Warning: [{year}/{semester}/{course_number}] Duplicate id"
                    f" {new_id} for {event}: {assigned_ids}"
                )
                new_id += 100

        assigned_ids[old_id] = new_id
        event["מס."] = new_id

        new_ids_events.setdefault(new_id, []).append(event)

    # Make sure each event of same category and id matches in all groups.
    for category in set(x["סוג"] for x in result):
        for id in set(x["מס."] for x in result if x["סוג"] == category):
            events_same_category_and_id = [
                x for x in result if x["סוג"] == category and x["מס."] == id
            ]
            event_groups = set(x["קבוצה"] for x in events_same_category_and_id)

            events_grouped = set()
            for group in event_groups:
                events_grouped.add(
                    frozenset(
                        [
                            tuple({**x, "קבוצה": None}.items())
                            for x in events_same_category_and_id
                            if x["קבוצה"] == group
                        ]
                    )
                )

            if len(events_grouped) != 1:
                raise RuntimeError(
                    f"Invalid events for category {category} and id {id}:"
                    f" {events_grouped}"
                )

    return result


def get_exam_date_time(exam_data: list[dict[str, Any]], exam_category: str):
    if len(set(x["ZzExamOfferGuid"] for x in exam_data if x["ZzExamOfferGuid"])) != len(
        exam_data
    ):
        raise RuntimeError(f"Duplicate exam ids: {exam_data}")

    # Make sure that each item is either root or has a parent which is root,
    # i.e. no more than one level.
    root_exam_ids = [
        x["ZzExamOfferGuid"] for x in exam_data if not x["ZzExamOfferParentGuid"]
    ]
    if any(
        x["ZzExamOfferParentGuid"] and x["ZzExamOfferParentGuid"] not in root_exam_ids
        for x in exam_data
    ):
        raise RuntimeError(f"Invalid parent exam: {exam_data}")

    # Sort by the order of root exams, place root items first.
    def exam_data_sort_key(exam):
        id = exam["ZzExamOfferParentGuid"]
        if not id:
            id = exam["ZzExamOfferGuid"]
        return root_exam_ids.index(id), exam["ZzExamOfferParentGuid"] != ""

    result_items = []
    dates_with_time = set()

    for exam in sorted(exam_data, key=exam_data_sort_key):
        if exam["CategoryCode"] != exam_category:
            if exam["CategoryCode"] not in ["FI", "FB", "MI", "M2"]:
                raise RuntimeError(f"Invalid category: {exam['CategoryCode']}")
            continue

        date_raw = exam["ExamDate"]
        if not date_raw:
            continue

        date = sap_date_parse(date_raw).strftime("%d-%m-%Y")

        time_begin_raw = exam["ExamBegTime"]
        match = re.fullmatch(r"PT(\d\d)H(\d\d)M\d\dS", time_begin_raw)
        if not match:
            raise RuntimeError(f"Invalid time: {time_begin_raw}")

        time_begin = match.group(1) + ":" + match.group(2)

        time_end_raw = exam["ExamEndTime"]
        match = re.fullmatch(r"PT(\d\d)H(\d\d)M\d\dS", time_end_raw)
        if not match:
            raise RuntimeError(f"Invalid time: {time_end_raw}")

        time_end = match.group(1) + ":" + match.group(2)

        time = f"{time_begin} - {time_end}"

        if exam["ZzExamOfferParentGuid"] == "" or time == "00:00 - 00:00":
            date_and_time = date
        else:
            date_and_time = f"{date} {time}"
            dates_with_time.add(date)

        result_items.append(date_and_time)

    # Remove dates that also have items with time.
    result_items = [x for x in result_items if x not in dates_with_time]

    # Remove duplicates while keeping order.
    result_items = list(dict.fromkeys(result_items))

    return "\n".join(result_items)


def get_course_full_data(year: int, semester: int, course_number: str):
    sap_course = get_sap_course(year, semester, course_number)

    course_number = sap_course["Otjid"]
    if course_number.startswith("SM"):
        course_number = course_number.removeprefix("SM")
    else:
        raise RuntimeError(f"Invalid course number: {course_number}")

    points = sap_course["Points"]
    points = re.sub(r"(\.[1-9]+)0+$", r"\1", points)
    points = re.sub(r"\.0+$", r"", points)

    responsible = ""
    for person in sap_course["Responsible"]["results"]:
        responsible += f"{person['Title']} {person['FirstName']} {person['LastName']}\n"
    responsible = responsible.rstrip("\n")

    rel = []
    rel_including = []
    rel_included = []
    for rel_item in sap_course["SmRelations"]["results"]:
        rel_course_number = rel_item["Otjid"].removeprefix("SM")
        if rel_item["ZzRelationshipKey"] == "AZEC":
            rel.append(rel_course_number)
        elif rel_item["ZzRelationshipKey"] == "AZCC":
            rel_including.append(rel_course_number)
        elif rel_item["ZzRelationshipKey"] == "BZCC":
            rel_included.append(rel_course_number)
        elif rel_item["ZzRelationshipKey"] == "AZID":
            # How is it different than AZEC? In previous data they were both in
            # the same entry.
            rel.append(rel_course_number)
        else:
            raise RuntimeError(f"Invalid relationship: {rel_item['ZzRelationshipKey']}")

    prereq = ""
    for prereq_item in sap_course["SmPrereq"]["results"]:
        prereq += prereq_item["Bracket"]
        if prereq_item["ModuleId"].lstrip("0"):
            prereq += prereq_item["ModuleId"]
        if prereq_item["Operator"] == "AND":
            prereq += f" ו-"
        elif prereq_item["Operator"] == "OR":
            prereq += f" או "
        elif prereq_item["Operator"]:
            raise RuntimeError(f"Invalid operator: {prereq_item['Operator']}")
    prereq = re.sub(r"\((\d+)\)", r"\1", prereq)
    prereq = re.sub(r"^\(([^()]+)\)$", r"\1", prereq)

    adjoining = []
    if match := re.search(
        r"^(?:מקצוע צמוד|מקצועות צמודים):(.*)",
        sap_course["ZzSemesterNote"],
        flags=re.MULTILINE,
    ):
        for adjoining_course in match.group(1).split(","):
            adjoining_course = adjoining_course.strip()
            if not re.fullmatch(r"\d{5,8}", adjoining_course):
                raise RuntimeError(f"Invalid adjoining course: {adjoining_course}")

            if len(adjoining_course) <= 6:
                adjoining_course = adjoining_course.zfill(6)
                adjoining_course = to_new_course_number(adjoining_course)
            else:
                adjoining_course = adjoining_course.zfill(8)

            adjoining.append(adjoining_course)

    exam_data = sap_course["Exams"]["results"]

    general = {
        "מספר מקצוע": course_number,
        "שם מקצוע": sap_course["Name"],
        "סילבוס": sap_course["StudyContentDescription"],
        "פקולטה": sap_course["OrgText"],
        "מסגרת לימודים": sap_course["ZzAcademicLevelText"],
    }

    if prereq:
        general["מקצועות קדם"] = prereq

    if adjoining:
        general["מקצועות צמודים"] = " ".join(adjoining)

    if rel:
        general["מקצועות ללא זיכוי נוסף"] = " ".join(rel)

    if rel_including:
        general["מקצועות ללא זיכוי נוסף (מכילים)"] = " ".join(rel_including)

    if rel_included:
        general["מקצועות ללא זיכוי נוסף (מוכלים)"] = " ".join(rel_included)

    general.update(
        {
            "נקודות": points,
            "אחראים": responsible,
            "הערות": sap_course["ZzSemesterNote"],
        }
    )

    exams = {
        "מועד א": get_exam_date_time(exam_data, "FI"),
        "מועד ב": get_exam_date_time(exam_data, "FB"),
        "מועד ג": "",  # TODO
        "בוחן מועד א": get_exam_date_time(exam_data, "MI"),
        "בוחן מועד ב": get_exam_date_time(exam_data, "M2"),
        "בוחן מועד ג": "",  # TODO
        "בוחן מועד ד": "",  # TODO
        "בוחן מועד ה": "",  # TODO
        "בוחן מועד ו": "",  # TODO
    }

    for exam, exam_date_time in exams.items():
        if exam_date_time:
            general[exam] = exam_date_time

    schedule = get_course_schedule(year, semester, course_number)

    return {
        "general": general,
        "schedule": schedule,
    }


def get_course_full_data_star(args):
    try:
        return get_course_full_data(*args)
    except Exception:
        print(f"Failed to get course data for {args}")
        raise


def postprocess_2024_200_201(result: list[dict], output_file: Path):
    unprocessed_file = output_file.with_stem(f"{output_file.stem}.unfiltered")
    output_file.rename(unprocessed_file)

    result = result.copy()
    for item in result:
        course = item["general"]["מספר מקצוע"]

        # Requested to be filtered by the faculty.
        is_math_course = course.startswith("0104") or course.startswith("0106")
        if is_math_course:
            schedule = []
            for s in item["schedule"]:
                if s["קבוצה"] in [
                    # סינים (סטודנטים סינים שלומדים בסין).
                    77,
                    # לימודי חוץ. זו קבוצה פיקטיבית.
                    69,
                    # יש את זה רק במושגי יסוד במתמטיקה, מדובר על תוכנית אודיסאה
                    # של תלמידי תיכון שלומדים בטכניון.
                    40,
                    # בינלאומי.
                    80,
                    86,
                ]:
                    continue
                schedule.append(s)
            item["schedule"] = schedule

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    return result


def run(
    year: int,
    semester: int,
    output_file: Path,
    min_js_output_file: Optional[Path] = None,
    run_postprocessing: bool = False,
):
    print(f'Fetching data for {year}-{semester}...')

    course_numbers = sorted(get_sap_course_numbers(year, semester))

    with Pool(POOL_CONCURRENT_PROCESSES) as pool:
        args = list(
            zip(
                repeat(year),
                repeat(semester),
                course_numbers,
            )
        )
        result = list(tqdm(pool.imap(get_course_full_data_star, args), total=len(args)))

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)

    if run_postprocessing:
        if year == 2024 and semester in [200, 201]:
            result = postprocess_2024_200_201(result, output_file)

    if min_js_output_file:
        with min_js_output_file.open("w", encoding="utf-8") as f:
            f.write("var courses_from_rishum = ")
            json.dump(result, f, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("year_and_semester")
    parser.add_argument("output_file")
    parser.add_argument("--min-js-output-file", default=None)
    parser.add_argument("--last-semesters-output-file", default=None)
    parser.add_argument("--run-postprocessing", action="store_true")
    args = parser.parse_args()

    year_and_semester = args.year_and_semester.split("-")
    if len(year_and_semester) != 2:
        raise RuntimeError(f"Invalid year_and_semester: {year_and_semester}")

    start = time.time()

    if year_and_semester[0] == "last":
        semester_count = int(year_and_semester[1])
        last_semesters = get_last_semesters(semester_count)

        if args.last_semesters_output_file:
            with Path(args.last_semesters_output_file).open("w", encoding="utf-8") as f:
                json.dump(last_semesters, f, indent=2, ensure_ascii=False)

        for last_semester in last_semesters:
            year = last_semester["year"]
            semester = last_semester["semester"]
            output_file = Path(args.output_file.format(year=year, semester=semester))
            min_js_output_file = (
                Path(args.min_js_output_file.format(year=year, semester=semester))
                if args.min_js_output_file
                else None
            )
            run(
                year, semester, output_file, min_js_output_file, args.run_postprocessing
            )
    else:
        year = int(year_and_semester[0])
        semester = int(year_and_semester[1])
        output_file = Path(args.output_file)
        min_js_output_file = (
            Path(args.min_js_output_file) if args.min_js_output_file else None
        )
        run(year, semester, output_file, min_js_output_file, args.run_postprocessing)

    end = time.time()
    print(f"Completed in {(end - start) / 60:.2f} minutes")


if __name__ == "__main__":
    main()
