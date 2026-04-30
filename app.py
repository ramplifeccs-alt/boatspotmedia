from app import app

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8080)

# BoatSpotMedia payments routes registration v41.5
try:
    from app.routes.payments import payments_bp
    app.register_blueprint(payments_bp)
except Exception as e:
    try:
        print("payments blueprint registration warning:", e)
    except Exception:
        pass
