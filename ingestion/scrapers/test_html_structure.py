import requests
from bs4 import BeautifulSoup

url = 'https://operations.nfl.com/inside-football-ops/rules-enforcement/gameday-accountability/'
data = requests.get(url).text
soup = BeautifulSoup(data, 'html.parser')

# Check for the class you're using
divs_with_select_table = soup.find_all('div', class_='select-table__table')
print(f"Found {len(divs_with_select_table)} divs with class 'select-table__table'")

# Look for divs that might contain weekly data
all_divs_with_class = soup.find_all('div', class_=True)
print(f"\nTotal divs with classes: {len(all_divs_with_class)}")

# Look for common patterns
print("\nSearching for divs with 'week' or 'table' in class name:")
for div in all_divs_with_class[:50]:  # Check first 50
    classes = div.get('class', [])
    class_str = ' '.join(classes)
    if 'week' in class_str.lower() or 'table' in class_str.lower():
        print(f"  Class: {class_str}")
        if div.get('data-week'):
            print(f"    -> Has data-week attribute: {div.get('data-week')}")

# Look for tables directly
tables = soup.find_all('table')
print(f"\nFound {len(tables)} table elements")

# Check if there's any data-week attribute anywhere
elements_with_week_attr = soup.find_all(attrs={'data-week': True})
print(f"\nFound {len(elements_with_week_attr)} elements with data-week attribute")
for elem in elements_with_week_attr[:5]:
    print(f"  {elem.name} with data-week='{elem.get('data-week')}' and classes: {elem.get('class')}")
