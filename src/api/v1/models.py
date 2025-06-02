from typing import Annotated, Any, List, Literal, Optional, Union

from pydantic import BaseModel, Extra, Field, EmailStr


class EmailOtp(BaseModel):
    email: EmailStr
    full_name: str | None = None