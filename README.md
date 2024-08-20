# technion-sap-info-fetcher

A script to fetch and parse Technion SAP courses information, to have it in an
accessible format.

A successor of
[technion-ug-info-fetcher](https://github.com/michael-maltsev/technion-ug-info-fetcher).

## The data

The script runs on a regular basis, and the data can be found in the [gh-pages
branch](https://github.com/michael-maltsev/technion-sap-info-fetcher/tree/gh-pages).

## Usage

```
courses_to_json.py 2024-200 courses.json
```

Specify the desired year and semester in the following format: `YYYY-SSS`, for
example `2024-200` for a Winter 2024-2025 semester. `200` and `201` mean Winter
and Spring, respectively.

The result will be saved in the specified JSON file.

## Example

An example of a course entry:

```json
{
  "general": {
    "מספר מקצוע": "02340124",
    "שם מקצוע": "מבוא לתכנות מערכות",
    "סילבוס": "השלמות שפת C: מצביעים, רשומות, ניהול זיכרון דינמי, רשימות מקושרות, עצים. ניהול גרסאות. הידור, קישור, ושימוש בספריות. פקודות LLEHS בסיסיות. פייתון כשפת \"דבק\" של המערכת. ניפוי שגיאות, בדיקת תוכנה, בדיקה אוטומטית. מבוא ל- C++ : תכנות מונחה עצמים, טיפוסי נתונים מופשטים, פולימורפיזם דינמי וסטטי.",
    "פקולטה": "הפקולטה למדעי המחשב",
    "מסגרת לימודים": "קדם אקדמי/תיכוני",
    "מקצועות קדם": "(02340114) או (02340117)",
    "מקצועות ללא זיכוי נוסף": "00440101 00940219 01040824 02340121 02340122",
    "נקודות": "4",
    "אחראים": "",
    "הערות": ""
  },
  "schedule": [
    {
      "קבוצה": 11,
      "סוג": "הרצאה",
      "יום": "ראשון",
      "שעה": "14:30 - 16:30",
      "בניין": "",
      "חדר": 0,
      "מרצה/מתרגל": "ד\"ר יוסף ויינשטיין",
      "מס.": 10
    },
    {
      "קבוצה": 11,
      "סוג": "תרגול",
      "יום": "חמישי",
      "שעה": "12:30 - 14:30",
      "בניין": "",
      "חדר": 0,
      "מרצה/מתרגל": "",
      "מס.": 11
    },
    {
      "קבוצה": 12,
      "סוג": "הרצאה",
      "יום": "ראשון",
      "שעה": "14:30 - 16:30",
      "בניין": "",
      "חדר": 0,
      "מרצה/מתרגל": "ד\"ר יוסף ויינשטיין",
      "מס.": 10
    },
    {
      "קבוצה": 12,
      "סוג": "תרגול",
      "יום": "שני",
      "שעה": "10:30 - 12:30",
      "בניין": "",
      "חדר": 0,
      "מרצה/מתרגל": "",
      "מס.": 12
    },
    ...
  ]
}
```
