import argparse
import json
import re
import time
import urllib.parse
from datetime import datetime, timezone
from functools import cache
from pathlib import Path
from typing import Any

import requests

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
    print(f"Got {len(json_str)} bytes")

    return json.loads(json_str)


def send_request(query: str):
    delay = 5
    while True:
        try:
            return send_request_once(query)
        except Exception as e:
            print(f"Error: {e}")
            time.sleep(delay)
            delay = min(delay * 2, 300)


def get_last_semesters(semester_count: int):
    params = {
        "sap-client": "700",
        "$select": ",".join(
            [
                "PiqYear",
                "PiqSession",
            ]
        ),
        # "$inlinecount": "allpages",
    }
    raw_data = send_request(f"SemesterSet?{urllib.parse.urlencode(params)}")
    raw_results = raw_data["d"]["results"]

    results = []
    for result in raw_results:
        year = int(result["PiqYear"])
        semester = int(result["PiqSession"])
        if semester not in [200, 201, 202]:
            continue

        results.append((year, semester))

    return sorted(results, reverse=True)[:semester_count]


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

    return building


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

    for raw_schedule in raw_schedule_results:
        group_id = int(raw_schedule["ZzSeSeqnr"])

        raw_schedule_items = raw_schedule["EObjectSet"]["results"]
        for raw_schedule_item in raw_schedule_items:
            category = raw_schedule_item["CategoryText"]

            building = ""
            room = 0
            building_and_room = raw_schedule_item["RoomText"]
            if match := re.fullmatch(r"(\d\d\d)-(\d\d\d\d)", building_and_room):
                building = get_building_name(
                    year, semester, raw_schedule_item["RoomId"]
                )
                room = int(match.group(2))
            elif building_and_room == "ראה פרטים":
                # TODO
                print(f"Warning: Unsupported building and room: {building_and_room}")
            elif building_and_room:
                raise RuntimeError(f"Invalid building and room: {building_and_room}")

            staff = ""
            for person in raw_schedule_item["Persons"]["results"]:
                staff += (
                    f"{person['Title']} {person['FirstName']} {person['LastName']}\n"
                )
            staff = staff.rstrip("\n")

            date_and_time_list = raw_schedule_item["ScheduleSummary"]
            if date_and_time_list != raw_schedule_item["ScheduleText"]:
                raise RuntimeError(
                    f"Date and time mismatch: {date_and_time_list} !="
                    f" {raw_schedule_item['ScheduleText']}"
                )

            if not date_and_time_list:
                continue

            if date_and_time_list == "לֹא סָדִיר":
                # TODO
                print("Warning: Unsupported date and time: 'לֹא סָדִיר'")
                continue

            # Skip specific dates like: "27.05.: 10:00-12:00".
            if re.fullmatch(r"\d\d\.\d\d\.: \d\d:\d\d-\d\d:\d\d", date_and_time_list):
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
                time = match.group(2) + " - " + match.group(3)

                result_item = {
                    "קבוצה": group_id,
                    "סוג": category,
                    "יום": day,
                    "שעה": time,
                    "בניין": building,
                    "חדר": room,
                    "מרצה/מתרגל": staff,
                }

                if result_item not in result:
                    result.append(result_item)

    has_lecture_mix = (
        len(
            set(x["קבוצה"] for x in result if x["סוג"] == "הרצאה")
            & set(x["קבוצה"] for x in result if x["סוג"] != "הרצאה")
        )
        > 0
    )

    lectures_by_id = {}
    for item in result:
        if item["סוג"] != "הרצאה" or not has_lecture_mix:
            item["מס."] = item["קבוצה"]
            continue

        item["מס."] = (item["קבוצה"] // 10) * 10
        lectures_by_id.setdefault(item["מס."], []).append(item)

    # Make sure each lecture of same id matches in all groups, and fill missing
    # data.
    for item in result:
        if item["סוג"] != "הרצאה" or not has_lecture_mix:
            continue

        lectures_same_id = lectures_by_id[item["מס."]]
        lectures_groups = set(x["קבוצה"] for x in lectures_same_id)
        lectures_same_date_time = [
            x
            for x in lectures_same_id
            if x["יום"] == item["יום"] and x["שעה"] == item["שעה"]
        ]

        if len(lectures_groups) != len(lectures_same_date_time):
            raise RuntimeError(
                f"Invalid number of matched lectures: {len(lectures_groups)} !="
                f" {len(lectures_same_date_time)}"
            )

        for lecture in lectures_same_date_time:
            if item.keys() != lecture.keys():
                raise RuntimeError(f"Invalid keys: {item.keys()} != {lecture.keys()}")

            for key in item.keys() - {"מס.", "קבוצה", "סוג", "יום", "שעה"}:
                if item[key] == lecture[key] or not lecture[key]:
                    continue

                if not item[key]:
                    # Copy missing value.
                    item[key] = lecture[key]
                    continue

                raise RuntimeError(f"Invalid value: {item[key]} != {lecture[key]}")

    return result


def get_exam_date_time(exam_data: list[dict[str, Any]], exam_category: str):
    result = ""

    for exam in exam_data:
        if exam["CategoryCode"] != exam_category:
            if exam["CategoryCode"] not in ["FI", "FB", "MI"]:
                raise RuntimeError(f"Invalid category: {exam['CategoryCode']}")
            continue

        date_raw = exam["ExamDate"]
        if not date_raw:
            continue

        match = re.fullmatch(r"/Date\((\d+)\)/", date_raw)
        if not match:
            raise RuntimeError(f"Invalid date: {date_raw}")

        date = datetime.fromtimestamp(int(match.group(1)) / 1000, timezone.utc)
        date = date.strftime("%d-%m-%Y")

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

        if time == "00:00 - 00:00":
            date_and_time = date
        else:
            date_and_time = f"{date} {time}"

        if not result:
            result = date_and_time
        elif result != date_and_time:
            raise RuntimeError(f"Date and time mismatch: {result} != {date_and_time}")

    return result


def get_course_full_data(year: int, semester: int, sap_course: dict[str, Any]):
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

    adjoining = []
    if match := re.search(
        r"^(?:מקצוע צמוד|מקצועות צמודים):(.*)",
        sap_course["ZzSemesterNote"],
        flags=re.MULTILINE,
    ):
        for adjoining_course in match.group(1).split(","):
            adjoining_course = adjoining_course.strip()
            match = re.fullmatch(r"(\d\d\d)(\d\d\d)", adjoining_course)
            if not match:
                raise RuntimeError(f"Invalid adjoining course: {adjoining_course}")

            # Convert from old format, hopefully that's always correct.
            adjoining.append(f"0{match.group(1)}0{match.group(2)}")

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
        "בוחן מועד ב": "",  # TODO
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


def run(year: int, semester: int, output_file: Path):
    result = []

    course_numbers = sorted(get_sap_course_numbers(year, semester))

    i = 0
    for course_number in course_numbers:
        i += 1
        print(f"Processing {i}/{len(course_numbers)}: {course_number}")
        result.append(
            get_course_full_data(
                year, semester, get_sap_course(year, semester, course_number)
            )
        )

    with output_file.open("w", encoding="utf-8") as f:
        json.dump(result, f, indent=2, ensure_ascii=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("year_and_semester")
    parser.add_argument("output_file")
    args = parser.parse_args()

    year_and_semester = args.year_and_semester.split("-")
    if len(year_and_semester) != 2:
        raise RuntimeError(f"Invalid year_and_semester: {year_and_semester}")

    start = time.time()

    if year_and_semester[0] == "last":
        semester_count = int(year_and_semester[1])
        for year, semester in get_last_semesters(semester_count):
            print(f"Getting courses for year: {year}, semester: {semester}")
            output_file = Path(args.output_file.format(year=year, semester=semester))
            run(year, semester, output_file)
    else:
        year = int(year_and_semester[0])
        semester = int(year_and_semester[1])
        output_file = Path(args.output_file)
        run(year, semester, output_file)

    end = time.time()
    print(f"Completed in {(end - start) / 60:.2f} minutes")


if __name__ == "__main__":
    main()