import flask
from flask import Flask
from flask import request
from flask import abort
from RestApi import Solution
import json

app = Flask(__name__)


@app.route('/top-airports', methods=["GET"])
def top_airports():
    top = int(request.args.get("N"))
    if top < 0:
        abort("The N value should be greater than or equal to 1:",400)
    get_top_airports = Solution().top_bookings(top)
    return json.dumps(get_top_airports)


if __name__ == '__main__':
    app.run(debug=True)
