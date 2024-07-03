from enum import Enum


class CategorySeq(Enum):
    EDUCATION_EXPERIENCE = 1
    KOREAN_MUSIC = 2
    SOLO_PERFORMANCE = 3
    DANCE = 4
    MUSICAL_OPERA = 5
    PLAY = 6
    MOVIE = 7
    EXHIBITION_ART = 8
    FESTIVAL_OTHER = 9
    FESTIVAL_CULTURE_ART = 10
    FESTIVAL_CITIZEN_HARMONY = 11
    FESTIVAL_NATURE_SCENERY = 12
    FESTIVAL_TRADITION_HISTORY = 13
    CONCERT = 14
    CLASSIC = 15
    OTHER = 16


class CategoryLabel(Enum):
    EDUCATION_EXPERIENCE = "교육/체험"
    KOREAN_MUSIC = "국악"
    SOLO_PERFORMANCE = "독주/독창회"
    DANCE = "무용"
    MUSICAL_OPERA = "뮤지컬/오페라"
    PLAY = "연극"
    MOVIE = "영화"
    EXHIBITION_ART = "전시/미술"
    FESTIVAL_OTHER = "축제-기타"
    FESTIVAL_CULTURE_ART = "축제-문화/예술"
    FESTIVAL_CITIZEN_HARMONY = "축제-시민화합"
    FESTIVAL_NATURE_SCENERY = "축제-자연/경관"
    FESTIVAL_TRADITION_HISTORY = "축제-전통/역사"
    CONCERT = "콘서트"
    CLASSIC = "클래식"
    OTHER = "기타"
