from datetime import datetime, timedelta
import pytz
from typing import Optional, Union, Dict

class TimeUtils:
    """프로젝트 전체에서 사용할 시간 관련 유틸리티 클래스"""

    DEFAULT_TIMEZONE = 'Asia/Seoul'

    @staticmethod
    def get_current_time(timezone: Optional[str] = None, use_timezone: bool = True) -> datetime:
        """현재 시간을 반환하는 함수

        Args:
            timezone (Optional[str], optional): 
                사용할 타임존. Defaults to None (Asia/Seoul 사용).
                pytz.all_timezones에서 지원하는 타임존 문자열.
            use_timezone (bool, optional):
                True: 타임존이 적용된 시간 반환
                False: 타임존 없는 기본 datetime.now() 반환
                Defaults to True.

        Returns:
            datetime: 현재 시간
                - use_timezone=True: 타임존이 적용된 datetime 객체
                - use_timezone=False: 타임존이 없는 기본 datetime 객체

        Example:            ```python
            # 기본 사용 (서울 시간)
            current_time = TimeUtils.get_current_time()

            # 특정 타임존 사용
            ny_time = TimeUtils.get_current_time('America/New_York')

            # 타임존 없는 기본 시간
            simple_time = TimeUtils.get_current_time(use_timezone=False)            ```
        """
        if not use_timezone:
            return datetime.now()
        
        tz = pytz.timezone(timezone or TimeUtils.DEFAULT_TIMEZONE)
        return datetime.now(tz)

    @staticmethod
    def convert_timezone(
        dt: datetime,
        to_timezone: Optional[str] = None,
        from_timezone: Optional[str] = None
    ) -> datetime:
        """한 타임존의 시간을 다른 타임존으로 변환

        Args:
            dt (datetime): 변환할 datetime 객체
            to_timezone (Optional[str], optional): 
                변환할 타임존. Defaults to None (Asia/Seoul 사용)
            from_timezone (Optional[str], optional): 
                원본 시간의 타임존. Defaults to None (UTC 사용)

        Returns:
            datetime: 변환된 시간

        Example:            ```python
            # UTC를 서울 시간으로 변환
            utc_time = datetime.now(pytz.UTC)
            seoul_time = TimeUtils.convert_timezone(utc_time)

            # 뉴욕 시간을 도쿄 시간으로 변환
            ny_time = datetime.now(pytz.timezone('America/New_York'))
            tokyo_time = TimeUtils.convert_timezone(
                ny_time, 
                to_timezone='Asia/Tokyo',
                from_timezone='America/New_York'
            )            ```
        """
        # 기본값 설정
        to_tz = pytz.timezone(to_timezone or TimeUtils.DEFAULT_TIMEZONE)
        from_tz = pytz.timezone(from_timezone or 'UTC')

        # 원본 시간에 타임존 정보가 없으면 추가
        if dt.tzinfo is None:
            dt = from_tz.localize(dt)

        # 타임존 변환
        return dt.astimezone(to_tz)

    @staticmethod
    def get_delta_time(
        days: int = 0,
        hours: int = 0,
        minutes: int = 0,
        timezone: Optional[str] = None,
        use_timezone: bool = True
    ) -> datetime:
        """현재 시간으로부터 지정된 시간 차이를 계산

        Args:
            days (int): 일 수
            hours (int): 시간
            minutes (int): 분
            timezone (Optional[str]): 사용할 타임존
            use_timezone (bool): 타임존 사용 여부

        Returns:
            datetime: 계산된 시간

        Example:            ```python
            # 3일 전 시간
            three_days_ago = TimeUtils.get_delta_time(days=-3)

            # 2시간 후 시간 (타임존 없이)
            two_hours_later = TimeUtils.get_delta_time(
                hours=2, 
                use_timezone=False
            )

            # 30분 전 뉴욕 시간
            ny_30min_ago = TimeUtils.get_delta_time(
                minutes=-30,
                timezone='America/New_York'
            )            ```
        """
        # 먼저 delta 계산
        delta = timedelta(days=days, hours=hours, minutes=minutes)
        
        if not use_timezone:
            return datetime.now() + delta
        
        # 타임존 적용
        tz = pytz.timezone(timezone or TimeUtils.DEFAULT_TIMEZONE)
        return datetime.now(tz) + delta

    @staticmethod
    def get_time_range(
        start_delta: Dict[str, int],
        end_delta: Optional[Dict[str, int]] = None,
        timezone: Optional[str] = None,
        use_timezone: bool = True
    ) -> tuple[datetime, datetime]:
        """시작과 끝 시간 범위를 계산

        Args:
            start_delta (Dict[str, int]): 시작 시간 delta
                {'days': -1, 'hours': -3, 'minutes': -30}
            end_delta (Optional[Dict[str, int]]): 종료 시간 delta
                기본값은 현재 시간
            timezone (Optional[str]): 사용할 타임존
            use_timezone (bool): 타임존 사용 여부

        Returns:
            tuple[datetime, datetime]: (시작 시간, 종료 시간)

        Example:            ```python
            # 3일 전부터 현재까지
            start_delta = {'days': -3}
            start_time, end_time = TimeUtils.get_time_range(start_delta)

            # 1시간 전부터 1시간 후까지
            start_delta = {'hours': -1}
            end_delta = {'hours': 1}
            start_time, end_time = TimeUtils.get_time_range(
                start_delta, 
                end_delta
            )            ```
        """
        start_time = TimeUtils.get_delta_time(
            **start_delta,
            timezone=timezone,
            use_timezone=use_timezone
        )
        
        if end_delta:
            end_time = TimeUtils.get_delta_time(
                **end_delta,
                timezone=timezone,
                use_timezone=use_timezone
            )
        else:
            end_time = TimeUtils.get_current_time(
                timezone=timezone,
                use_timezone=use_timezone
            )
            
        return start_time, end_time    

    
if __name__ == '__main__' :
    
    # 기본 사용 (서울 시간)
    current_time = TimeUtils.get_current_time()
    # 타임존 없는 기본 시간
    simple_time = TimeUtils.get_current_time(use_timezone=False)
    # 특정 타임존 시간
    ny_time = TimeUtils.get_current_time('America/New_York')
    # 시간 변환
    utc_to_seoul = TimeUtils.convert_timezone(datetime.now(pytz.UTC))

    print(current_time)
    print(simple_time)
    print(ny_time)
    print(utc_to_seoul)

    # 단순 delta 계산
    three_days_ago = TimeUtils.get_delta_time(days=-3)

    # 시간 범위 계산
    start_delta = {'days': -7, 'hours': -3}  # 7일 3시간 전
    end_delta = {'days': -1}    # 1일 전
    start_time, end_time = TimeUtils.get_time_range(
        start_delta,
        end_delta,
        timezone='Asia/Seoul'
    )

    print(three_days_ago)
    print(start_time)
    print(end_time)

    pass