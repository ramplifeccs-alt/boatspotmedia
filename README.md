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
