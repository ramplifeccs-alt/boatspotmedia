# BoatSpotMedia Final Test Build

This is a complete testable Flask build for BoatSpotMedia.

## Included

- Public homepage
- Public search by location/date/time
- Creator application
- Hidden creator login route
- Hidden owner login route
- Creator dashboard with left menu
- Creator upload page
- 128GB batch limit
- Storage limit enforcement
- Cloudflare R2 multi-bucket support
- ffprobe creation_time extraction
- ffmpeg center thumbnail generation
- Buyer preview pages
- Buyer cart with second-clip discount
- Buyer orders/download token foundation
- Owner panel approval workflow
- Owner storage plan creation
- Temporary commission override
- Advertiser PPC dashboard
- PPC balance auto-pause
- Charter dashboard and public charters
- Global creator click analytics

## Railway

Set environment variables from `.env.example`.

## Important for Railway ffmpeg

This build calls ffmpeg/ffprobe. If Railway image does not include them, add a Nixpacks config or Dockerfile installing ffmpeg.

## Default owner

The app seeds:
email: owner@boatspotmedia.com
password: ChangeMe123!

Change immediately after real auth is completed.

## Test routes

Public:
- /
- /search
- /apply-creator
- /services
- /charters

Hidden/internal:
- /creator/login
- /creator/dashboard
- /creator/upload
- /creator/batches
- /creator/settings
- /owner/login
- /owner/panel
- /advertiser/dashboard
- /charters/dashboard


## v3 fix

This package includes automatic startup migrations for older Railway PostgreSQL tables.
If Railway still shows database column errors from very old test builds, the fastest clean test option is:
- Railway PostgreSQL → Data → remove old tables, or create a fresh PostgreSQL database.
- Redeploy this package.


## v4 fix

Adds automatic repair for older `video` table columns:
- recorded_at
- file_size_bytes
- r2 keys
- status
- price fields

Also protects homepage from crashing if old DB tables are incomplete.


## v5 fix

Adds auto-repair for creator_application social columns:
instagram, facebook, youtube, tiktok.
Also expands auto-repair to all major test tables.


## v6 Apply Creator DB repair

If your Railway DB was created by old packages, open:
/owner/repair-db-now

Then submit /apply-creator again.
The apply route also repairs creator_application immediately before saving.


## v7 creator application SQL fix

Creator applications now save with direct SQL after force repairing the table.
Check saved applications at:
/owner/applications-raw


## v8 brand_name fix

Fixes legacy Railway DB where creator_application.brand_name is NOT NULL.
Apply form now includes Creator / Brand Name and insert includes brand_name.


## v9 Instagram-only creator application

Creator application now uses only Instagram.
brand_name is automatically saved from Instagram without @.


## v10 social_link fix

Fixes legacy Railway DB where creator_application.social_link is NOT NULL.
The app now saves social_link using the cleaned Instagram username.


## v11 Owner Applications Panel

New routes:
- /owner/applications
- POST /owner/applications/<id>/approve
- POST /owner/applications/<id>/reject

Approving an application creates/activates a creator user and CreatorProfile.


## v12 Corrections
Creator panel reorganized: Uploads, Batches, Orders, Products, Pricing, Settings. Second clip discount is video-only. Orders show filename.


## v13 Creator Panel Fix
Fixes Batches/Orders robustness, edit/delete products and pricing, creator name/Instagram/sidebar/logout, plan storage display, published videos count.
