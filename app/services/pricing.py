def _num(value):
    try:
        if value is None:
            return None
        value = float(value)
        if value <= 0:
            return None
        return value
    except Exception:
        return None

def creator_video_price_options(creator=None, video=None):
    """
    Returns only the price options configured for the creator/video.
    If only original exists, buyer sees only Original.
    If edited and/or bundle exist, buyer sees those too.
    """
    original = None
    edited = None
    bundle = None

    if video is not None:
        original = _num(getattr(video, "original_price", None)) or _num(getattr(video, "price", None))
        edited = _num(getattr(video, "edited_price", None))
        bundle = _num(getattr(video, "bundle_price", None))

    if creator is not None:
        original = original or _num(getattr(creator, "original_price", None)) or _num(getattr(creator, "default_original_price", None)) or _num(getattr(creator, "instant_download_price", None))
        edited = edited or _num(getattr(creator, "edited_price", None)) or _num(getattr(creator, "default_edited_price", None))
        bundle = bundle or _num(getattr(creator, "bundle_price", None)) or _num(getattr(creator, "default_bundle_price", None))

        plan = getattr(creator, "plan", None)
        if plan is not None:
            original = original or _num(getattr(plan, "original_price", None)) or _num(getattr(plan, "default_original_price", None))
            edited = edited or _num(getattr(plan, "edited_price", None)) or _num(getattr(plan, "default_edited_price", None))
            bundle = bundle or _num(getattr(plan, "bundle_price", None)) or _num(getattr(plan, "default_bundle_price", None))

    options = []

    # Fallback default only if nothing is configured anywhere.
    if not original and not edited and not bundle:
        original = 50.0

    if original:
        options.append({
            "key": "original",
            "title": "Original",
            "price": original,
            "description": "Instant download of the original camera file. No editing, no delay."
        })

    if edited:
        options.append({
            "key": "edited",
            "title": "Edited Video",
            "price": edited,
            "description": "Edited version prepared for social media. Delivery is not instant."
        })

    if bundle:
        options.append({
            "key": "bundle",
            "title": "Original + Edited Video",
            "price": bundle,
            "description": "Includes instant original download plus an edited version delivered later."
        })

    return options
