VERSION = "2.0.0 beta :)"

import random, os, string, sys, json
import argparse
import atexit
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
import time
import user_agnt
from xvfbwrapper import Xvfb

BASE_DIR        = os.path.dirname(os.path.abspath(__file__))
PROXY_FILE      = os.path.join(BASE_DIR, "proxies.txt")
USED_PROXY_FILE = os.path.join(BASE_DIR, "used_proxies.txt")
LOG_FILE        = os.path.join(BASE_DIR, "ip_log.txt")
REQUEST_TIMEOUT = 15

def _test_proxy(proxy: str, timeout: int) -> bool:
    for url in ["http://api.ipify.org", "http://httpbin.org/ip", "http://ifconfig.me/ip"]:
        try:
            r = requests.get(url, proxies={"http": proxy, "https": proxy}, timeout=timeout)
            if r.status_code == 200:
                return True
        except Exception:
            continue
    return False

def get_working_proxy(proxy_list, timeout=REQUEST_TIMEOUT) -> str:
    random.shuffle(proxy_list)
    print(f"[~] Testing {len(proxy_list)} proxies...")
    working = []
    with ThreadPoolExecutor(max_workers=min(len(proxy_list), 20)) as ex:
        futures = {ex.submit(_test_proxy, p, timeout): p for p in proxy_list}
        for f in as_completed(futures):
            if f.result():
                working.append(futures[f])
    if not working:
        raise RuntimeError("No working proxies available!")
    chosen = random.choice(working)
    print(f"✅ {len(working)} working | 🎯 Selected: {chosen}")
    return chosen

def mark_proxy_used(proxy: str):
    with open(PROXY_FILE) as f:
        lines = [l.strip() for l in f if l.strip()]
    remaining = [l for l in lines if not (l == proxy or f"http://{l}" == proxy or l == proxy.replace("http://", ""))]
    with open(PROXY_FILE, "w") as f:
        f.write("\n".join(remaining) + ("\n" if remaining else ""))
    with open(USED_PROXY_FILE, "a") as f:
        f.write(proxy + "\n")
    print(f"[~] Proxy moved to used: {proxy}")

def get_ip_info(proxy_url: str = None, retries: int = 6, delay: int = 5) -> dict:
    proxies = {"http": proxy_url, "https": proxy_url} if proxy_url else None
    urls = ["http://ipwho.is/", "http://ip-api.com/json", "http://api.ipify.org?format=json"]
    for attempt in range(retries):
        for url in urls:
            try:
                r = requests.get(url, timeout=10, proxies=proxies)
                data = r.json()
                if data.get("ip") or data.get("query"):
                    data.setdefault("ip", data.get("query"))
                    data.setdefault("country_code", data.get("countryCode", "US"))
                    return data
            except Exception as e:
                print(f"[-] IP lookup failed ({url}): {e}")
        print(f"[~] Tor not ready yet, retrying in {delay}s... ({attempt+1}/{retries})")
        time.sleep(delay)
    return {}

CC_LANG = {
    "US": ("en-US", "America/New_York",      "en-US,en;q=0.9"),
    "GB": ("en-GB", "Europe/London",         "en-GB,en;q=0.9"),
    "IT": ("it-IT", "Europe/Rome",           "it-IT,it;q=0.9,en;q=0.8"),
    "DE": ("de-DE", "Europe/Berlin",         "de-DE,de;q=0.9,en;q=0.8"),
    "FR": ("fr-FR", "Europe/Paris",          "fr-FR,fr;q=0.9,en;q=0.8"),
    "ES": ("es-ES", "Europe/Madrid",         "es-ES,es;q=0.9,en;q=0.8"),
    "NL": ("nl-NL", "Europe/Amsterdam",      "nl-NL,nl;q=0.9,en;q=0.8"),
    "PL": ("pl-PL", "Europe/Warsaw",         "pl-PL,pl;q=0.9,en;q=0.8"),
    "BR": ("pt-BR", "America/Sao_Paulo",     "pt-BR,pt;q=0.9,en;q=0.8"),
    "RU": ("ru-RU", "Europe/Moscow",         "ru-RU,ru;q=0.9,en;q=0.8"),
    "TR": ("tr-TR", "Europe/Istanbul",       "tr-TR,tr;q=0.9,en;q=0.8"),
    "JP": ("ja-JP", "Asia/Tokyo",            "ja-JP,ja;q=0.9,en;q=0.8"),
    "CN": ("zh-CN", "Asia/Shanghai",         "zh-CN,zh;q=0.9,en;q=0.8"),
    "SE": ("sv-SE", "Europe/Stockholm",      "sv-SE,sv;q=0.9,en;q=0.8"),
    "MX": ("es-MX", "America/Mexico_City",   "es-MX,es;q=0.9,en;q=0.8"),
    "IN": ("en-IN", "Asia/Kolkata",          "en-IN,en;q=0.9,hi;q=0.8"),
    "AU": ("en-AU", "Australia/Sydney",      "en-AU,en;q=0.9"),
    "CA": ("en-CA", "America/Toronto",       "en-CA,en;q=0.9,fr;q=0.8"),
    "AR": ("es-AR", "America/Argentina/Buenos_Aires", "es-AR,es;q=0.9,en;q=0.8"),
    "UA": ("uk-UA", "Europe/Kiev",           "uk-UA,uk;q=0.9,en;q=0.8"),
    "RO": ("ro-RO", "Europe/Bucharest",      "ro-RO,ro;q=0.9,en;q=0.8"),
    "HU": ("hu-HU", "Europe/Budapest",       "hu-HU,hu;q=0.9,en;q=0.8"),
    "CZ": ("cs-CZ", "Europe/Prague",         "cs-CZ,cs;q=0.9,en;q=0.8"),
    "PT": ("pt-PT", "Europe/Lisbon",         "pt-PT,pt;q=0.9,en;q=0.8"),
    "GR": ("el-GR", "Europe/Athens",         "el-GR,el;q=0.9,en;q=0.8"),
    "ID": ("id-ID", "Asia/Jakarta",          "id-ID,id;q=0.9,en;q=0.8"),
    "TH": ("th-TH", "Asia/Bangkok",          "th-TH,th;q=0.9,en;q=0.8"),
    "VN": ("vi-VN", "Asia/Ho_Chi_Minh",      "vi-VN,vi;q=0.9,en;q=0.8"),
    "PH": ("en-PH", "Asia/Manila",           "en-PH,en;q=0.9,fil;q=0.8"),
    "ZA": ("en-ZA", "Africa/Johannesburg",   "en-ZA,en;q=0.9"),
    "NG": ("en-NG", "Africa/Lagos",          "en-NG,en;q=0.9"),
    "EG": ("ar-EG", "Africa/Cairo",          "ar-EG,ar;q=0.9,en;q=0.8"),
    "SA": ("ar-SA", "Asia/Riyadh",           "ar-SA,ar;q=0.9,en;q=0.8"),
    "IL": ("he-IL", "Asia/Jerusalem",        "he-IL,he;q=0.9,en;q=0.8"),
    "KR": ("ko-KR", "Asia/Seoul",            "ko-KR,ko;q=0.9,en;q=0.8"),
    "SG": ("en-SG", "Asia/Singapore",        "en-SG,en;q=0.9,zh;q=0.8"),
    "MY": ("ms-MY", "Asia/Kuala_Lumpur",     "ms-MY,ms;q=0.9,en;q=0.8"),
    "NO": ("nb-NO", "Europe/Oslo",           "nb-NO,nb;q=0.9,en;q=0.8"),
    "FI": ("fi-FI", "Europe/Helsinki",       "fi-FI,fi;q=0.9,en;q=0.8"),
    "DK": ("da-DK", "Europe/Copenhagen",     "da-DK,da;q=0.9,en;q=0.8"),
    "CH": ("de-CH", "Europe/Zurich",         "de-CH,de;q=0.9,en;q=0.8"),
    "AT": ("de-AT", "Europe/Vienna",         "de-AT,de;q=0.9,en;q=0.8"),
    "BE": ("fr-BE", "Europe/Brussels",       "fr-BE,fr;q=0.9,nl;q=0.8,en;q=0.7"),
    # Africa
    "MA": ("ar-MA", "Africa/Casablanca",     "ar-MA,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    "DZ": ("ar-DZ", "Africa/Algiers",        "ar-DZ,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    "TN": ("ar-TN", "Africa/Tunis",          "ar-TN,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    "KE": ("en-KE", "Africa/Nairobi",        "en-KE,en;q=0.9,sw;q=0.8"),
    "GH": ("en-GH", "Africa/Accra",          "en-GH,en;q=0.9"),
    "ET": ("am-ET", "Africa/Addis_Ababa",    "am-ET,am;q=0.9,en;q=0.8"),
    "TZ": ("sw-TZ", "Africa/Dar_es_Salaam",  "sw-TZ,sw;q=0.9,en;q=0.8"),
    "SN": ("fr-SN", "Africa/Dakar",          "fr-SN,fr;q=0.9,en;q=0.8"),
    "CI": ("fr-CI", "Africa/Abidjan",        "fr-CI,fr;q=0.9,en;q=0.8"),
    "CM": ("fr-CM", "Africa/Douala",         "fr-CM,fr;q=0.9,en;q=0.8"),
    # Middle East
    "AE": ("ar-AE", "Asia/Dubai",            "ar-AE,ar;q=0.9,en;q=0.8"),
    "IQ": ("ar-IQ", "Asia/Baghdad",          "ar-IQ,ar;q=0.9,en;q=0.8"),
    "IR": ("fa-IR", "Asia/Tehran",           "fa-IR,fa;q=0.9,en;q=0.8"),
    "KW": ("ar-KW", "Asia/Kuwait",           "ar-KW,ar;q=0.9,en;q=0.8"),
    "QA": ("ar-QA", "Asia/Qatar",            "ar-QA,ar;q=0.9,en;q=0.8"),
    "JO": ("ar-JO", "Asia/Amman",            "ar-JO,ar;q=0.9,en;q=0.8"),
    "LB": ("ar-LB", "Asia/Beirut",           "ar-LB,ar;q=0.9,fr;q=0.8,en;q=0.7"),
    # Asia
    "PK": ("ur-PK", "Asia/Karachi",          "ur-PK,ur;q=0.9,en;q=0.8"),
    "BD": ("bn-BD", "Asia/Dhaka",            "bn-BD,bn;q=0.9,en;q=0.8"),
    "LK": ("si-LK", "Asia/Colombo",          "si-LK,si;q=0.9,en;q=0.8"),
    "NP": ("ne-NP", "Asia/Kathmandu",        "ne-NP,ne;q=0.9,en;q=0.8"),
    "MM": ("my-MM", "Asia/Rangoon",          "my-MM,my;q=0.9,en;q=0.8"),
    "KH": ("km-KH", "Asia/Phnom_Penh",       "km-KH,km;q=0.9,en;q=0.8"),
    "HK": ("zh-HK", "Asia/Hong_Kong",        "zh-HK,zh;q=0.9,en;q=0.8"),
    "TW": ("zh-TW", "Asia/Taipei",           "zh-TW,zh;q=0.9,en;q=0.8"),
    "MN": ("mn-MN", "Asia/Ulaanbaatar",      "mn-MN,mn;q=0.9,en;q=0.8"),
    "UZ": ("uz-UZ", "Asia/Tashkent",         "uz-UZ,uz;q=0.9,ru;q=0.8,en;q=0.7"),
    "KZ": ("kk-KZ", "Asia/Almaty",           "kk-KZ,kk;q=0.9,ru;q=0.8,en;q=0.7"),
    "AZ": ("az-AZ", "Asia/Baku",             "az-AZ,az;q=0.9,ru;q=0.8,en;q=0.7"),
    "GE": ("ka-GE", "Asia/Tbilisi",          "ka-GE,ka;q=0.9,en;q=0.8"),
    "AM": ("hy-AM", "Asia/Yerevan",          "hy-AM,hy;q=0.9,en;q=0.8"),
    # Europe (extra)
    "SK": ("sk-SK", "Europe/Bratislava",     "sk-SK,sk;q=0.9,en;q=0.8"),
    "SI": ("sl-SI", "Europe/Ljubljana",      "sl-SI,sl;q=0.9,en;q=0.8"),
    "HR": ("hr-HR", "Europe/Zagreb",         "hr-HR,hr;q=0.9,en;q=0.8"),
    "RS": ("sr-RS", "Europe/Belgrade",       "sr-RS,sr;q=0.9,en;q=0.8"),
    "BG": ("bg-BG", "Europe/Sofia",          "bg-BG,bg;q=0.9,en;q=0.8"),
    "LT": ("lt-LT", "Europe/Vilnius",        "lt-LT,lt;q=0.9,en;q=0.8"),
    "LV": ("lv-LV", "Europe/Riga",           "lv-LV,lv;q=0.9,en;q=0.8"),
    "EE": ("et-EE", "Europe/Tallinn",        "et-EE,et;q=0.9,en;q=0.8"),
    "BY": ("be-BY", "Europe/Minsk",          "be-BY,be;q=0.9,ru;q=0.8,en;q=0.7"),
    "MD": ("ro-MD", "Europe/Chisinau",       "ro-MD,ro;q=0.9,ru;q=0.8,en;q=0.7"),
    "MK": ("mk-MK", "Europe/Skopje",         "mk-MK,mk;q=0.9,en;q=0.8"),
    "AL": ("sq-AL", "Europe/Tirane",         "sq-AL,sq;q=0.9,en;q=0.8"),
    "IE": ("en-IE", "Europe/Dublin",         "en-IE,en;q=0.9"),
    "IS": ("is-IS", "Atlantic/Reykjavik",    "is-IS,is;q=0.9,en;q=0.8"),
    # Americas
    "CL": ("es-CL", "America/Santiago",      "es-CL,es;q=0.9,en;q=0.8"),
    "CO": ("es-CO", "America/Bogota",        "es-CO,es;q=0.9,en;q=0.8"),
    "PE": ("es-PE", "America/Lima",          "es-PE,es;q=0.9,en;q=0.8"),
    "VE": ("es-VE", "America/Caracas",       "es-VE,es;q=0.9,en;q=0.8"),
    "EC": ("es-EC", "America/Guayaquil",     "es-EC,es;q=0.9,en;q=0.8"),
    "BO": ("es-BO", "America/La_Paz",        "es-BO,es;q=0.9,en;q=0.8"),
    "PY": ("es-PY", "America/Asuncion",      "es-PY,es;q=0.9,en;q=0.8"),
    "UY": ("es-UY", "America/Montevideo",    "es-UY,es;q=0.9,en;q=0.8"),
    "CR": ("es-CR", "America/Costa_Rica",    "es-CR,es;q=0.9,en;q=0.8"),
    "GT": ("es-GT", "America/Guatemala",     "es-GT,es;q=0.9,en;q=0.8"),
    "CU": ("es-CU", "America/Havana",        "es-CU,es;q=0.9,en;q=0.8"),
    "DO": ("es-DO", "America/Santo_Domingo", "es-DO,es;q=0.9,en;q=0.8"),
    "NZ": ("en-NZ", "Pacific/Auckland",      "en-NZ,en;q=0.9"),
    # Oceania / Pacific
    "PG": ("en-PG", "Pacific/Port_Moresby",  "en-PG,en;q=0.9"),
    "FJ": ("en-FJ", "Pacific/Fiji",          "en-FJ,en;q=0.9"),
}

_parser = argparse.ArgumentParser(add_help=False)
_parser.add_argument("-T", "--tor",        action="store_true")
_parser.add_argument("-P", "--proxy",      action="store_true")
_parser.add_argument("--debug",            action="store_true")
_parser.add_argument("--socks-port",       type=int, default=int(os.environ.get("SOCKS_PORT", 9050)))
_parser.add_argument("--control-port",     type=int, default=int(os.environ.get("CONTROL_PORT", 9051)))
_parser.add_argument("--api-port",         type=int, default=int(os.environ.get("API_PORT", 5000)))
_args, _ = _parser.parse_known_args()

TOR_PROXY    = f"socks5://127.0.0.1:{_args.socks_port}"
CONTROL_PORT = _args.control_port
API_BASE     = f"http://127.0.0.1:{_args.api_port}"

def tor_reset_and_get_ip() -> str:
    # wait for bootstrap via API
    for _ in range(24):  # 2 min max
        try:
            r = requests.get(f"{API_BASE}/status", timeout=5)
            if r.json().get("bootstrapped"):
                break
        except Exception:
            pass
        print(f"[~] Waiting for Tor API on {API_BASE}...")
        time.sleep(5)
    # reset IP via API
    try:
        requests.get(f"{API_BASE}/reset-ip", timeout=5)
    except Exception as e:
        print(f"[-] reset-ip failed: {e}")
    time.sleep(5)
    return get_ip_info(TOR_PROXY).get("ip", "unknown")

def log_ip(ip: str):
    with open(LOG_FILE, "a") as f:
        f.write(f"{time.strftime('%Y-%m-%d %H:%M:%S')} | {ip}\n")

def generate_identity():
    name   = ''.join(random.choices(string.ascii_lowercase, k=8))
    number = random.randint(1000, 9999)
    email  = f"kalawssimatrix+{number}@gmail.com"
    print("=" * 40)
    print(f"  Name : {name}")
    print(f"  Email: {email}")
    print("=" * 40)
    return name, email

VIEWPORTS = [
    {"width": 1920, "height": 1080},
    {"width": 1366, "height": 768},
    {"width": 1536, "height": 864},
    {"width": 1440, "height": 900},
    {"width": 1280, "height": 720},
]

def run_session(elements: dict, session_id: int = 0, proxy_config: dict = None):
    tag = f"[{session_id % 100:02d}]"
    _print = lambda msg: print(f"{tag} {msg}")
    if _args.tor:
        tor_ip = tor_reset_and_get_ip()
        log_ip(tor_ip)
        proxy_config = proxy_config or {"server": TOR_PROXY}
    else:
        tor_ip = "N/A"
        proxy_config = proxy_config or {}

    proxy_url = proxy_config.get("server") if proxy_config else None
    ip_info   = get_ip_info(proxy_url)
    cc        = (ip_info.get("country_code") or "US").upper()
    locale, chosen_tz, _ = CC_LANG.get(cc, CC_LANG["US"])
    lang_primary = locale
    lang_base    = locale.split("-")[0]
    _print(f"IP: {ip_info.get('ip')} | {ip_info.get('country')} ({cc}) → locale={locale} tz={chosen_tz}")
    os.environ["TZ"] = chosen_tz
    time.tzset()

    session_profile = os.path.join(BASE_DIR, f"playwright-profile-{session_id}")
    # clean profile before AND register cleanup on crash
    import shutil
    def _cleanup():
        shutil.rmtree(session_profile, ignore_errors=True)
    atexit.register(_cleanup)
    _cleanup()
    os.makedirs(session_profile, exist_ok=True)

    EMAIL = elements.get("email")
    NAME  = elements.get("name", "N/A")

    # Playwright version → Chromium version mapping
    PLAYWRIGHT_CHROMIUM_VERSION = {
        "1.44": "124", "1.43": "123", "1.42": "122", "1.41": "121",
        "1.40": "120", "1.39": "119",
    }
    import importlib.metadata
    _pw_ver = ".".join(importlib.metadata.version("playwright").split(".")[:2])
    _chrome_ver = PLAYWRIGHT_CHROMIUM_VERSION.get(_pw_ver, "124")

    # Desktop UAs from user_agnt + version-matched Chrome fallbacks
    DESKTOP_UAS = list(user_agnt.user_agent_list) or [
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36",
        f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36 Edg/{_chrome_ver}.0.0.0",
        f"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36",
        f"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Safari/537.36",
    ]

    # Mobile device profiles: (ua, viewport, touch, webgl_vendor, webgl_renderer, hw, mem, platform, ua_platform, ua_platform_ver, model)
    MOBILE_PROFILES = [
        (f"Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 393, "height": 851},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 730, OpenGL ES 3.2)", 8, 8, "Linux armv8l", "Android", "13.0.0", "Pixel 7"),
        (f"Mozilla/5.0 (Linux; Android 13; Pixel 6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 412, "height": 915},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 650, OpenGL ES 3.2)", 8, 8, "Linux armv8l", "Android", "13.0.0", "Pixel 6"),
        (f"Mozilla/5.0 (Linux; Android 12; SM-G991B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 360, "height": 800},  True, "Google Inc.", "ANGLE (ARM, Mali-G78, OpenGL ES 3.2)", 8, 8, "Linux armv8l", "Android", "12.0.0", "SM-G991B"),
        (f"Mozilla/5.0 (Linux; Android 13; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 360, "height": 780},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 740, OpenGL ES 3.2)", 8, 12, "Linux armv8l", "Android", "13.0.0", "SM-S918B"),
        (f"Mozilla/5.0 (Linux; Android 12; Redmi Note 11) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{_chrome_ver}.0.0.0 Mobile Safari/537.36",
         {"width": 393, "height": 873},  True, "Google Inc.", "ANGLE (Qualcomm, Adreno (TM) 680, OpenGL ES 3.2)", 8, 6, "Linux armv8l", "Android", "12.0.0", "Redmi Note 11"),
        ("Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
         {"width": 390, "height": 844},  True, "Apple Inc.", "Apple A16 GPU", 6, 6, "iPhone", "iOS", "17.0.0", "iPhone"),
        ("Mozilla/5.0 (iPhone; CPU iPhone OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
         {"width": 375, "height": 812},  True, "Apple Inc.", "Apple A15 GPU", 6, 6, "iPhone", "iOS", "16.6.0", "iPhone"),
        ("Mozilla/5.0 (iPad; CPU OS 16_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Mobile/15E148 Safari/604.1",
         {"width": 820, "height": 1180}, True, "Apple Inc.", "Apple M1 GPU", 8, 8, "iPad",   "iOS", "16.6.0", "iPad"),
    ]

    # 70% desktop, 30% mobile — mirrors real traffic split
    is_mobile = random.random() < 0.30

    with sync_playwright() as p:
        browser_type = p.chromium

        if is_mobile:
            mob = random.choice(MOBILE_PROFILES)
            (chrome_ua, chosen_viewport, _has_touch,
             _webgl_v, _webgl_r, _hw, _mem,
             _platform, _ua_platform, _ua_platform_ver, _model) = mob
            is_ios = "iPhone" in chrome_ua or "iPad" in chrome_ua
        else:
            chosen_viewport = random.choice(VIEWPORTS)
            chrome_ua       = random.choice(DESKTOP_UAS)
            is_ios          = False

        _print("\n" + "═" * 52)
        _print(f"  SESSION #{session_id}")
        _print("═" * 52)
        _print(f"  {'Version':<14}: {VERSION}")
        _print(f"  {'Device':<14}: {'Mobile' if is_mobile else 'Desktop'}")
        _print(f"  {'Name':<14}: {NAME}")
        _print(f"  {'Email':<14}: {EMAIL}")
        _print(f"  {'Proxy':<14}: {proxy_url or TOR_PROXY}")
        _print(f"  {'Tor IP':<14}: {tor_ip}")
        _print(f"  {'User-Agent':<14}: {chrome_ua[:55]}...")
        _print(f"  {'Viewport':<14}: {chosen_viewport['width']}x{chosen_viewport['height']}")
        _print(f"  {'Timezone':<14}: {chosen_tz}")
        _print(f"  {'Debug':<14}: {_args.debug}")
        _print("═" * 52 + "\n")

        launch_kwargs = dict(
            headless=False,
            proxy=proxy_config,
            user_agent=chrome_ua,
            viewport=chosen_viewport,
            locale=locale,
            timezone_id=chosen_tz,
            has_touch=is_mobile,
            is_mobile=is_mobile,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-infobars",
                "--disable-dev-shm-usage",
                f"--window-size={chosen_viewport['width']},{chosen_viewport['height']}",
            ],
            ignore_default_args=["--enable-automation"],
        )
        if os.path.exists("/usr/bin/google-chrome"):
            launch_kwargs["channel"] = "chrome"

        context = browser_type.launch_persistent_context(session_profile, **launch_kwargs)
        try:
            page = context.pages[0] if context.pages else context.new_page()

            hw_concurrency  = _hw  if is_mobile else random.choice([2, 4, 8])
            device_memory   = _mem if is_mobile else random.choice([2, 4, 8])
            battery_level   = round(random.uniform(0.6, 1.0), 2)
            battery_charging = random.choice(["true", "false"])
            rtt             = random.choice([50, 100, 150])
            downlink        = random.choice([5, 10, 20])
            canvas_salt     = random.randint(1, 255)
            audio_salt      = round(random.uniform(0.001, 0.009), 4)
            tz_offset_map   = {
                "America/New_York": 300, "America/Sao_Paulo": 180, "America/Mexico_City": 360,
                "Europe/London": 0, "Europe/Rome": -60, "Europe/Berlin": -60,
                "Europe/Paris": -60, "Europe/Madrid": -60, "Europe/Amsterdam": -60,
                "Europe/Warsaw": -60, "Europe/Moscow": -180, "Europe/Istanbul": -180,
                "Europe/Stockholm": -60, "Asia/Tokyo": -540, "Asia/Shanghai": -480,
            }
            tz_offset = tz_offset_map.get(chosen_tz, 0)
            webgl_vendors  = [
                ("Intel Inc.",            "Intel Iris OpenGL Engine"),
                ("Intel Inc.",            "Intel Iris Pro OpenGL Engine"),
                ("Intel Inc.",            "Intel HD Graphics 630"),
                ("Intel Inc.",            "Intel UHD Graphics 770"),
                ("Intel Inc.",            "Intel Iris Plus Graphics"),
                ("Intel",                 "Intel(R) UHD Graphics 630"),
                ("Intel",                 "Intel(R) UHD Graphics 620"),
                ("Intel",                 "Intel(R) HD Graphics 520"),
                ("Intel",                 "Intel(R) HD Graphics 620"),
                ("Intel",                 "Intel(R) Iris(R) Xe Graphics"),
                ("Intel",                 "Intel(R) Iris(R) Plus Graphics 640"),
                ("Google Inc.",           "ANGLE (Intel, Mesa Intel(R) UHD Graphics 620, OpenGL 4.6)"),
                ("Google Inc.",           "ANGLE (Intel, Mesa Intel(R) UHD Graphics 630, OpenGL 4.6)"),
                ("Google Inc.",           "ANGLE (Intel, Intel(R) UHD Graphics 630 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (Intel, Intel(R) UHD Graphics 620 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (Intel, Intel(R) Iris(R) Xe Graphics Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (Intel, Intel(R) Iris(R) Plus Graphics Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (NVIDIA, NVIDIA GeForce GTX 1050 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (NVIDIA, NVIDIA GeForce GTX 1650 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (NVIDIA, NVIDIA GeForce GTX 1660 Ti Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (NVIDIA, NVIDIA GeForce RTX 2070 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (NVIDIA, NVIDIA GeForce RTX 3060 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (NVIDIA, NVIDIA GeForce RTX 3080 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (NVIDIA, NVIDIA GeForce RTX 4070 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (AMD, AMD Radeon RX 570 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (AMD, AMD Radeon RX 580 Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (AMD, AMD Radeon RX 6600 XT Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (AMD, AMD Radeon RX 6700 XT Direct3D11 vs_5_0 ps_5_0)"),
                ("Google Inc.",           "ANGLE (AMD, AMD Radeon(TM) Graphics Direct3D11 vs_5_0 ps_5_0)"),
                ("NVIDIA Corporation",    "NVIDIA GeForce GTX 1050 Ti/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce GTX 1060/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce GTX 1650/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce GTX 1660 Ti/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce RTX 2060/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce RTX 2070/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce RTX 3060/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce RTX 3070/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce RTX 3080/PCIe/SSE2"),
                ("NVIDIA Corporation",    "NVIDIA GeForce RTX 4070/PCIe/SSE2"),
                ("ATI Technologies Inc.", "AMD Radeon Pro 5500M OpenGL Engine"),
                ("ATI Technologies Inc.", "AMD Radeon Pro 5600M OpenGL Engine"),
                ("ATI Technologies Inc.", "AMD Radeon Pro 560X OpenGL Engine"),
                ("ATI Technologies Inc.", "AMD Radeon RX 570"),
                ("ATI Technologies Inc.", "AMD Radeon RX 580"),
                ("ATI Technologies Inc.", "AMD Radeon RX 6600 XT"),
                ("ATI Technologies Inc.", "AMD Radeon RX 6700 XT"),
                ("Apple Inc.",            "Apple M1"),
                ("Apple Inc.",            "Apple M1 Pro"),
                ("Apple Inc.",            "Apple M1 Max"),
                ("Apple Inc.",            "Apple M2"),
                ("Apple Inc.",            "Apple M2 Pro"),
                ("Apple Inc.",            "Apple M3"),
                ("Qualcomm",              "Adreno (TM) 618"),
                ("Qualcomm",              "Adreno (TM) 640"),
                ("Qualcomm",              "Adreno (TM) 650"),
                ("Qualcomm",              "Adreno (TM) 660"),
                ("ARM",                   "Mali-G78"),
                ("ARM",                   "Mali-G710"),
            ]
            webgl_vendor, webgl_renderer = (_webgl_v, _webgl_r) if is_mobile else random.choice(webgl_vendors)

            # Derive platform string from UA for consistency
            if is_mobile:
                _platform        = _platform
                _ua_platform     = _ua_platform
                _ua_platform_ver = _ua_platform_ver
            elif "Windows" in chrome_ua:
                _platform = "Win32"
                _ua_platform = "Windows"
                _ua_platform_ver = "10.0.0"
            elif "Macintosh" in chrome_ua:
                _platform = "MacIntel"
                _ua_platform = "macOS"
                _ua_platform_ver = "13.0.0"
            else:
                _platform = "Linux x86_64"
                _ua_platform = "Linux"
                _ua_platform_ver = "5.15.0"

            page.add_init_script(f"""
                // --- automation flags ---
                Object.defineProperty(navigator, 'webdriver', {{ get: () => undefined }});

                // --- window.chrome (realistic) ---
                window.chrome = {{
                    app: {{ isInstalled: false, InstallState: {{}}, RunningState: {{}} }},
                    csi: () => ({{pageT: Date.now(), startE: Date.now(), tran: 15}}),
                    loadTimes: () => ({{
                        commitLoadTime: Date.now()/1000 - 0.4,
                        connectionInfo: 'h2',
                        finishDocumentLoadTime: Date.now()/1000 - 0.1,
                        finishLoadTime: Date.now()/1000,
                        firstPaintAfterLoadTime: 0,
                        firstPaintTime: Date.now()/1000 - 0.3,
                        navigationType: 'Other',
                        npnNegotiatedProtocol: 'h2',
                        requestTime: Date.now()/1000 - 0.5,
                        startLoadTime: Date.now()/1000 - 0.5,
                        wasAlternateProtocolAvailable: false,
                        wasFetchedViaSpdy: true,
                        wasNpnNegotiated: true,
                    }}),
                    runtime: {{
                        OnInstalledReason: {{}},
                        OnRestartRequiredReason: {{}},
                        PlatformArch: {{}},
                        PlatformNaclArch: {{}},
                        PlatformOs: {{}},
                        RequestUpdateCheckStatus: {{}},
                        connect: () => {{}},
                        sendMessage: () => {{}},
                    }},
                }};

                // --- plugins (realistic PDF plugin) ---
                const _fakeMime = {{ type: 'application/pdf', suffixes: 'pdf', description: 'Portable Document Format', enabledPlugin: null }};
                const _fakePlugin = {{
                    name: 'PDF Viewer', filename: 'internal-pdf-viewer',
                    description: 'Portable Document Format',
                    length: 1, 0: _fakeMime,
                    item: (i) => i === 0 ? _fakeMime : null,
                    namedItem: (n) => n === 'application/pdf' ? _fakeMime : null,
                    [Symbol.iterator]: function*() {{ yield _fakeMime; }}
                }};
                _fakeMime.enabledPlugin = _fakePlugin;
                const _pluginArray = {{
                    length: 1, 0: _fakePlugin,
                    item: (i) => i === 0 ? _fakePlugin : null,
                    namedItem: (n) => n === 'PDF Viewer' ? _fakePlugin : null,
                    refresh: () => {{}},
                    [Symbol.iterator]: function*() {{ yield _fakePlugin; }}
                }};
                Object.defineProperty(navigator, 'plugins',   {{ get: () => _pluginArray }});
                Object.defineProperty(navigator, 'mimeTypes', {{ get: () => ({{
                    length: 1, 0: _fakeMime,
                    item: (i) => i === 0 ? _fakeMime : null,
                    namedItem: (n) => n === 'application/pdf' ? _fakeMime : null,
                    [Symbol.iterator]: function*() {{ yield _fakeMime; }}
                }})}});

                // --- platform ---
                Object.defineProperty(navigator, 'platform', {{ get: () => '{_platform}' }});

                // --- userAgentData ---
                Object.defineProperty(navigator, 'userAgentData', {{ get: () => ({{
                    brands: [
                        {{ brand: 'Chromium',       version: '{_chrome_ver}' }},
                        {{ brand: 'Google Chrome',  version: '{_chrome_ver}' }},
                        {{ brand: 'Not=A?Brand',    version: '99'            }},
                    ],
                    mobile: {'true' if is_mobile else 'false'},
                    platform: '{_ua_platform}',
                    getHighEntropyValues: (hints) => Promise.resolve({{
                        architecture: {'""' if is_mobile else '"x86"'},
                        bitness: {'""' if is_mobile else '"64"'},
                        brands: [
                            {{ brand: 'Chromium',      version: '{_chrome_ver}' }},
                            {{ brand: 'Google Chrome', version: '{_chrome_ver}' }},
                            {{ brand: 'Not=A?Brand',   version: '99'            }},
                        ],
                        fullVersionList: [
                            {{ brand: 'Chromium',      version: '{_chrome_ver}.0.0.0' }},
                            {{ brand: 'Google Chrome', version: '{_chrome_ver}.0.0.0' }},
                            {{ brand: 'Not=A?Brand',   version: '99.0.0.0'            }},
                        ],
                        mobile: {'true' if is_mobile else 'false'},
                        model: '{"" if not is_mobile else _model}',
                        platform: '{_ua_platform}',
                        platformVersion: '{_ua_platform_ver}',
                        uaFullVersion: '{_chrome_ver}.0.0.0',
                        wow64: false,
                    }}),
                }})}});

                // --- touch support ---
                {'Object.defineProperty(navigator, "maxTouchPoints", { get: () => 5 });' if is_mobile else 'Object.defineProperty(navigator, "maxTouchPoints", { get: () => 0 });'}
                {'window.ontouchstart = null;' if is_mobile else ''}

                // --- language / locale ---
                Object.defineProperty(navigator, 'languages', {{ get: () => ['{lang_primary}', '{lang_base}'] }});
                Object.defineProperty(navigator, 'language',  {{ get: () => '{lang_primary}' }});

                // --- window geometry ---
                Object.defineProperty(window, 'outerHeight', {{ get: () => window.innerHeight + 85 }});
                Object.defineProperty(window, 'outerWidth',  {{ get: () => window.innerWidth  }});
                Object.defineProperty(screen, 'width',       {{ get: () => {chosen_viewport['width']}  }});
                Object.defineProperty(screen, 'height',      {{ get: () => {chosen_viewport['height']} }});
                Object.defineProperty(screen, 'availWidth',  {{ get: () => {chosen_viewport['width']}  }});
                Object.defineProperty(screen, 'availHeight', {{ get: () => {chosen_viewport['height']} }});
                Object.defineProperty(screen, 'colorDepth',  {{ get: () => 24 }});
                Object.defineProperty(screen, 'pixelDepth',  {{ get: () => 24 }});

                // --- hardware ---
                Object.defineProperty(navigator, 'hardwareConcurrency', {{ get: () => {hw_concurrency} }});
                Object.defineProperty(navigator, 'deviceMemory',        {{ get: () => {device_memory}  }});

                // --- canvas noise (pixel-level) ---
                const _toDataURL = HTMLCanvasElement.prototype.toDataURL;
                HTMLCanvasElement.prototype.toDataURL = function(...args) {{
                    const ctx = this.getContext('2d');
                    if (ctx) {{
                        const img = ctx.getImageData(0, 0, this.width || 1, this.height || 1);
                        img.data[0] = img.data[0] ^ {canvas_salt};
                        ctx.putImageData(img, 0, 0);
                    }}
                    return _toDataURL.apply(this, args);
                }};
                const _getImageData = CanvasRenderingContext2D.prototype.getImageData;
                CanvasRenderingContext2D.prototype.getImageData = function(...args) {{
                    const data = _getImageData.apply(this, args);
                    for (let i = 0; i < data.data.length; i += 100) {{
                        data.data[i] = data.data[i] ^ {canvas_salt};
                    }}
                    return data;
                }};

                // --- WebGL ---
                const _getParam = WebGLRenderingContext.prototype.getParameter;
                WebGLRenderingContext.prototype.getParameter = function(p) {{
                    if (p === 37445) return '{webgl_vendor}';
                    if (p === 37446) return '{webgl_renderer}';
                    return _getParam.call(this, p);
                }};
                const _getParam2 = WebGL2RenderingContext.prototype.getParameter;
                WebGL2RenderingContext.prototype.getParameter = function(p) {{
                    if (p === 37445) return '{webgl_vendor}';
                    if (p === 37446) return '{webgl_renderer}';
                    return _getParam2.call(this, p);
                }};

                // --- AudioContext noise ---
                const _createAnalyser = AudioContext.prototype.createAnalyser;
                AudioContext.prototype.createAnalyser = function() {{
                    const analyser = _createAnalyser.call(this);
                    const _getFloatFreq = analyser.getFloatFrequencyData.bind(analyser);
                    analyser.getFloatFrequencyData = function(arr) {{
                        _getFloatFreq(arr);
                        for (let i = 0; i < arr.length; i++) arr[i] += (Math.random() - 0.5) * {audio_salt};
                    }};
                    return analyser;
                }};

                // --- Timezone offset (DST-aware via real API) ---
                const _origGetTZOffset = Date.prototype.getTimezoneOffset;
                Date.prototype.getTimezoneOffset = function() {{
                    const jan = new Date(this.getFullYear(), 0, 1).getTime();
                    const jul = new Date(this.getFullYear(), 6, 1).getTime();
                    const stdOffset = {tz_offset};
                    const dstOffset = stdOffset - 60;
                    const isDST = this.getTime() < Math.max(jan, jul) && this.getTime() > Math.min(jan, jul);
                    return isDST ? dstOffset : stdOffset;
                }};

                // --- Battery ---
                navigator.getBattery = () => Promise.resolve({{
                    charging: {battery_charging}, level: {battery_level},
                    chargingTime: 0, dischargingTime: Infinity,
                    addEventListener: () => {{}}
                }});

                // --- Network ---
                Object.defineProperty(navigator, 'connection', {{ get: () => ({{
                    effectiveType: '4g', rtt: {rtt}, downlink: {downlink}, saveData: false,
                    addEventListener: () => {{}}
                }})}});

                // --- Permissions ---
                const _permQuery = navigator.permissions.query.bind(navigator.permissions);
                navigator.permissions.query = (p) =>
                    p.name === 'notifications'
                        ? Promise.resolve({{ state: 'default' }})
                        : _permQuery(p);

                // --- Speech synthesis ---
                Object.defineProperty(speechSynthesis, 'getVoices', {{ get: () => () => [] }});

                // --- Media devices ---
                if (navigator.mediaDevices) {{
                    navigator.mediaDevices.enumerateDevices = () => Promise.resolve([
                        {{ kind: 'audioinput',  label: '', deviceId: 'default', groupId: 'default' }},
                        {{ kind: 'audiooutput', label: '', deviceId: 'default', groupId: 'default' }},
                        {{ kind: 'videoinput',  label: '', deviceId: 'default', groupId: 'default' }},
                    ]);
                }}
            """)

            page.goto(f"https://mohmal.eu.org/?{EMAIL}", wait_until="domcontentloaded", timeout=60000)
            time.sleep(3)

            AD_LOG_FILE = os.path.join(BASE_DIR, "ads_log.jsonl")
            AD_NETWORKS = {
                "a-ads.com": "A-ADS", "acceptable.a-ads.com": "A-ADS",
                "googlesyndication.com": "Google AdSense",
                "doubleclick.net": "Google DFP",
                "adnxs.com": "AppNexus",
                "moatads.com": "Moat",
                "amazon-adsystem.com": "Amazon Ads",
                "media.net": "Media.net",
            }

            def detect_network(url: str) -> str:
                for domain, name in AD_NETWORKS.items():
                    if domain in url:
                        return name
                return None  # None = not an ad network

            # ── 1. DETECT — only iframes whose src belongs to a known ad network ──
            iframe_handles = page.locator("iframe").all()
            detected_ads = []
            for ih in iframe_handles:
                try:
                    src     = ih.get_attribute("src") or ""
                    data_aa = ih.get_attribute("data-aa") or ""
                    network = detect_network(src)
                    # include if known network OR has data-aa (A-ADS marker)
                    if not network and not data_aa:
                        continue
                    network = network or "A-ADS"
                    bb = ih.bounding_box()
                    detected_ads.append({
                        "data_aa": data_aa,
                        "src":     src,
                        "network": network,
                        "size":    f"{int(bb['width'])}x{int(bb['height'])}" if bb else "unknown",
                        "x": bb["x"] if bb else 0,
                        "y": bb["y"] if bb else 0,
                        "text": None, "click_url": None, "loaded": False,
                    })
                except Exception:
                    continue

            _print(f"\n── 1. DETECTED {len(detected_ads)} ad iframe(s) ─────────────")
            for ad in detected_ads:
                _print(f"   [{ad['network']}] id={ad['data_aa'] or 'n/a'} size={ad['size']}")

            # ── 2+3. VIEW + ANALYSE — one pass: scroll → hover → dwell → read ──
            _print("\n── 2+3. VIEW + ANALYSE ───────────────────────────────")
            for ad in detected_ads:
                try:
                    selector = f"iframe[data-aa='{ad['data_aa']}']" if ad["data_aa"] \
                               else f"iframe[src*='{ad['src'].lstrip('/').split('?')[0][:40]}']"
                    el = page.locator(selector).first
                    el.scroll_into_view_if_needed(timeout=5000)
                    time.sleep(random.uniform(0.5, 1.2))
                    bb = el.bounding_box()
                    if bb:
                        cx = bb["x"] + bb["width"]  * random.uniform(0.3, 0.7)
                        cy = bb["y"] + bb["height"] * random.uniform(0.3, 0.7)
                        page.mouse.move(cx, cy, steps=random.randint(8, 20))
                        dwell = random.uniform(3.0, 7.0)
                        _print(f"   👁  [{ad['network']}] #{ad['data_aa'] or 'n/a'} — dwell {dwell:.1f}s")
                        time.sleep(dwell)
                except Exception as e:
                    _print(f"   ⚠  Scroll/hover failed: {e}")

                # match frame — resolve // prefix and strip query string for matching
                src_key = ad["src"].lstrip("/").split("?")[0]
                ad_frame = None
                for f in page.frames:
                    if (ad["data_aa"] and ad["data_aa"] in f.url) or \
                       (src_key and src_key in f.url):
                        ad_frame = f
                        break
                if not ad_frame:
                    time.sleep(2)
                    for f in page.frames:
                        if (ad["data_aa"] and ad["data_aa"] in f.url) or \
                           (src_key and src_key in f.url):
                            ad_frame = f
                            break

                if ad_frame:
                    ad["loaded"] = True
                    for sel in ["a", ".aa-title", ".aa-description", "h1,h2,h3", "p", "body"]:
                        try:
                            el_f = ad_frame.locator(sel).first
                            text = el_f.inner_text(timeout=2000).strip()
                            href = el_f.get_attribute("href") if sel == "a" else None
                            if text:
                                ad["text"]      = text[:200]
                                ad["click_url"] = href
                                break
                        except Exception:
                            continue
                    ad["text"] = ad["text"] or "<empty>"
                else:
                    ad["text"] = "<frame not found>"

                status = "✅" if ad["loaded"] else "❌"
                _print(f"   {status} [{ad['network']}] #{ad['data_aa'] or 'n/a'}")
                _print(f"      Text : {ad['text']}")
                if ad["click_url"]:
                    _print(f"      Link : {ad['click_url'][:80]}")

            # ── 4. CLICK — 1 random loaded ad, catch new tab ──────────────
            _print("\n── 4. CLICK ──────────────────────────────────────────")
            clickable = [a for a in detected_ads if a["loaded"] and a["text"] not in ("<empty>", "<frame not found>")]
            if clickable and random.random() < 0.25:
                target = random.choice(clickable)
                try:
                    selector = f"iframe[data-aa='{target['data_aa']}']" if target["data_aa"] \
                               else f"iframe[src*='{target['src'].lstrip('/').split('?')[0][:40]}']"
                    el = page.locator(selector).first
                    el.scroll_into_view_if_needed(timeout=5000)
                    time.sleep(random.uniform(1.0, 2.5))
                    bb = el.bounding_box()
                    if bb:
                        cx = bb["x"] + bb["width"]  * random.uniform(0.3, 0.7)
                        cy = bb["y"] + bb["height"] * random.uniform(0.3, 0.7)
                        page.mouse.move(cx, cy, steps=random.randint(10, 25))
                        time.sleep(random.uniform(0.3, 0.8))

                        with context.expect_page(timeout=8000) as new_tab_info:
                            page.mouse.click(cx, cy)

                        new_tab = new_tab_info.value
                        _print(f"   🖱  Clicked ad #{target['data_aa'] or 'n/a'} [{target['network']}]")
                        try:
                            new_tab.wait_for_load_state("domcontentloaded", timeout=15000)
                        except Exception:
                            pass

                        tab_url   = new_tab.url
                        tab_title = "<unknown>"
                        tab_text  = "<none>"
                        try: tab_title = new_tab.title()
                        except Exception: pass
                        try: tab_text = new_tab.locator("h1, h2, h3, p").first.inner_text(timeout=3000).strip()[:200]
                        except Exception: pass

                        _print(f"   🆕 New tab:")
                        _print(f"      URL   : {tab_url[:100]}")
                        _print(f"      Title : {tab_title}")
                        _print(f"      Text  : {tab_text}")
                        target["landing_url"]   = tab_url
                        target["landing_title"] = tab_title
                        target["landing_text"]  = tab_text
                        dwell = random.uniform(5.0, 12.0)
                        _print(f"   ⏱  Dwelling {dwell:.1f}s...")
                        time.sleep(dwell)
                        new_tab.close()
                        _print("   ✅ Tab closed")
                except Exception as e:
                    _print(f"   ⚠  Click failed: {e}")
            else:
                _print("   ⏭  No click this session")

            # ── single log write at end with all data ─────────────────────
            with open(AD_LOG_FILE, "a") as f:
                f.write(json.dumps({
                    "ts":     time.strftime("%Y-%m-%d %H:%M:%S"),
                    "session": session_id,
                    "ip":     ip_info.get("ip"),
                    "cc":     cc,
                    "device": "mobile" if is_mobile else "desktop",
                    "ads":    detected_ads,
                }) + "\n")

            _print("─────────────────────────────────────────────────────\n")
            time.sleep(random.uniform(5.0, 10.0))
            _print(f"Current URL: {page.url}")



        finally:
            context.close()


# --- entry point ---
if not _args.tor and not _args.proxy:
    print("[-] Specify -T (Tor) or -P (proxy). Exiting.")
    sys.exit(1)

proxy_config = None
if _args.proxy:
    with open(PROXY_FILE) as f:
        proxies = [l.strip() if "://" in l.strip() else f"http://{l.strip()}" for l in f if l.strip()]
    if not proxies:
        print("[~] proxies.txt empty — restoring from used_proxies.txt")
        with open(USED_PROXY_FILE) as f:
            proxies = [l.strip() for l in f if l.strip()]
        with open(PROXY_FILE, "w") as f:
            f.write("\n".join(proxies) + "\n")
        open(USED_PROXY_FILE, "w").close()
        print(f"[~] Restored {len(proxies)} proxies")
    proxy_url = get_working_proxy(proxies)
    mark_proxy_used(proxy_url)
    print(f"[~] Using proxy: {proxy_url}")
    proxy_config = {"server": proxy_url}

name, email_addr = generate_identity()
session_id = _args.socks_port
with Xvfb(width=1920, height=1080, colordepth=24):
    run_session(elements={"email": email_addr, "name": name}, session_id=session_id, proxy_config=proxy_config)
