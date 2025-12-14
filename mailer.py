# mailer.py (SendGrid API 버전)
import os
import streamlit as st
import logging
from datetime import date, timedelta
from typing import List, Dict, Tuple
from collections import defaultdict
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Attachment, FileContent, FileName, FileType, Disposition
import base64

logger = logging.getLogger(__name__)

try:
    import config as _local_config
except ModuleNotFoundError:
    _local_config = None

def _cfg(key, default=None):
    if _local_config and hasattr(_local_config, key):
        return getattr(_local_config, key)
    try:
        return st.secrets[key]
    except Exception:
        return default

MAIL_FROM = _cfg("MAIL_FROM", "daegu_eers@naver.com")  # Single Sender 주소
MAIL_FROM_NAME = _cfg("MAIL_FROM_NAME", "대구본부 EERS팀")
SENDGRID_API_KEY = _cfg("SENDGRID_API_KEY", "")

SIX_MONTHS = timedelta(days=180)

# --------------------------------------------
# HTML 생성 함수들은 그대로 사용 (build_subject 등)
# --------------------------------------------

# SendGrid 전송 함수
def send_mail_sendgrid(
    to_list: List[str],
    subject: str,
    html_body: str,
    attach_name: str = None,
    attach_html: str = None,
):
    """SendGrid API 기반 메일 발송"""
    if not SENDGRID_API_KEY:
        st.error("⚠️ SENDGRID_API_KEY가 설정되어 있지 않습니다.")
        logger.error("SENDGRID_API_KEY missing")
        return False

    # 메시지 생성
    message = Mail(
        from_email=(MAIL_FROM, MAIL_FROM_NAME),
        to_emails=to_list,
        subject=subject,
        html_content=html_body,
    )

    # 첨부파일 추가
    if attach_name and attach_html:
        encoded = base64.b64encode(attach_html.encode("utf-8")).decode()
        attachment = Attachment()
        attachment.file_content = FileContent(encoded)
        attachment.file_type = FileType("text/html")
        attachment.file_name = FileName(attach_name)
        attachment.disposition = Disposition("attachment")
        message.attachment = attachment

    try:
        sg = SendGridAPIClient(SENDGRID_API_KEY)
        response = sg.send(message)
        if response.status_code in (200, 202):
            logger.info(f"메일 발송 성공 → {subject}")
            st.success("📨 메일이 성공적으로 발송되었습니다! (SendGrid)")
            return True
        else:
            st.error(f"메일 발송 실패: {response.status_code}")
            logger.error(f"SendGrid 응답 코드: {response.status_code}")
            return False
    except Exception as e:
        logger.error(f"[ERROR] SendGrid send_mail 실패: {e}")
        st.error(f"메일 발송 실패: {e}")
        return False


# 간단한 인증코드용 발송
def send_verification_email(to_email: str, code: str):
    html = f"<p>[EERS 시스템]</p><p>인증코드: <b>{code}</b></p>"
    return send_mail_sendgrid(
        to_list=[to_email],
        subject="[EERS 시스템] 로그인 인증코드",
        html_body=html,
    )
