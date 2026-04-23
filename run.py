
from flask import Flask, render_template

app = Flask(__name__)

@app.route("/")
def homepage():
    return render_template("public/homepage.html")

@app.route("/search")
def search():
    return render_template("public/search.html")

@app.route("/apply-creator")
def apply_creator():
    return render_template("public/apply_creator.html")

@app.route("/buyer/orders")
def buyer_orders():
    return render_template("buyer/orders.html")

@app.route("/creator/dashboard")
def creator_dashboard():
    return render_template("creator/dashboard.html")

@app.route("/owner/panel")
def owner_panel():
    return render_template("owner/panel.html")

@app.route("/services")
def services():
    return render_template("services/services.html")

@app.route("/charters")
def charters():
    return render_template("charters/charters.html")

if __name__ == "__main__":
    app.run(debug=True)
