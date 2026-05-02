# v41.9 Auth Rollback Safe

This package intentionally removes the custom buyer_account auth code introduced in v41.7/v41.8.

Reason:
- The project already had an existing auth/login system, including Google login.
- Replacing auth without inspecting the existing system caused login failures.

Kept:
- Header/logo color.
- Clickable logo.
- Payment success CTA.
- Cart/payment changes from v41.6 base.

Not included:
- New buyer_account table usage.
- Custom password hashing buyer login.
- Any changes to Google login.

Next safe step:
- Inspect the existing auth system and connect buyer order history to that system without replacing login routes or auth tables.
