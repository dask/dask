// TODO Animate workers when performing tasks or swapping to show activity
// TODO Add memory usage dial around outside of workers
// TODO Add clients
// TODO Show future retrieval
// TODO Show graph submission

const workerColor = "#ECB172";
const workerIdleSize = 12;
const workerBusySize = 15;

// Expose in the global scope
var dashboard;

class Dashboard {
  constructor() {
    this.workers = [];
    this.scheduler = "scheduler";
    this.schedulerNode = null;
    this.dashboard = document.getElementById("vis");
    this.add_scheduler();
  }

  handle_event(event) {
    switch (event["name"]) {
      case "pong":
        console.log(event);
        break;
      case "add_worker":
        this.add_worker(this.hash_worker(event["worker"]));
        break;
      case "remove_worker":
        this.remove_worker(this.hash_worker(event["worker"]));
        break;
      case "restart":
        this.reset();
        break;
      case "transition":
        if (event["action"] === "compute") {
          this.run_task(
            this.hash_worker(event["worker"]),
            event["key"],
            event["stop"] - event["start"],
            event["color"]
          );
          break;
        } else if (event["action"] === "transfer") {
          this.run_transfer(
            this.hash_worker(event["source"]),
            this.hash_worker(event["worker"]),
            event["stop"] - event["start"]
          );
          break;
        } else if (
          event["action"] === "disk-read" ||
          event["action"] === "disk-write"
        ) {
          this.run_swap(
            this.hash_worker(event["worker"]),
            event["stop"] - event["start"]
          );
          break;
        } else if (event["action"] === "deserialize") {
          this.run_deserialize(
            this.hash_worker(event["worker"]),
            event["stop"] - event["start"]
          );
          break;
        }
      default:
        console.log("Unknown event " + event["name"]);
        console.log(event);
    }
  }

  // Convert a worker address into a valid DOM ID
  hash_worker(worker) {
    let worker_id = "worker-" + worker.replace(/(:\/\/|\.|\:)/g, "-");
    return worker_id;
  }

  add_scheduler() {
    this.schedulerNode = document.createElementNS(
      "http://www.w3.org/2000/svg",
      "circle"
    );
    this.schedulerNode.setAttributeNS(null, "id", this.scheduler);
    this.schedulerNode.setAttributeNS(null, "class", "node scheduler");
    this.dashboard.appendChild(this.schedulerNode);
    anime({
      targets: "#" + this.scheduler,
      r: [0, 30],
      cx: ["50%", "50%"],
      cy: ["50%", "50%"],
      duration: 250
    });
  }

  add_worker(id) {
    // Create new circle element and add it to the SVG
    let workerNode = document.createElementNS(
      "http://www.w3.org/2000/svg",
      "circle"
    );
    workerNode.setAttributeNS(null, "id", id);
    workerNode.setAttributeNS(null, "r", "0");
    workerNode.setAttributeNS(null, "cx", "50%");
    workerNode.setAttributeNS(null, "cy", "50%");
    workerNode.setAttributeNS(null, "class", "node worker");
    this.dashboard.appendChild(workerNode);

    // Add our new worker to the list of workers and then reposition them all
    let position = Math.floor(Math.random() * this.workers.length) - 1;
    this.workers.splice(position, 0, id);
    this.update_worker_positions();
  }

  remove_worker(id) {
    // Remove circle element from SVG
    let worker = document.getElementById(id);
    anime({
      targets: worker,
      opacity: 0,
      duration: 1000,
      complete: () => this.dashboard.removeChild(worker)
    });

    // Remove worker from list of workers
    let index = this.workers.indexOf(id);
    if (index > -1) {
      this.workers.splice(index, 1);
    }

    // Reposition other workers to fill in the gap
    this.update_worker_positions();
  }

  connected() {
    anime({
      targets: "#scheduler",
      opacity: 1,
      duration: 1000
    });
  }

  disconnected() {
    while (this.workers.length > 0) {
      this.remove_worker(this.workers[0]);
    }
    anime({
      targets: "#scheduler",
      opacity: 0.3,
      duration: 1000
    });
  }

  update_worker_positions() {
    // Calculate a circle around the scheduler and position our workers equally around it
    for (var i = 0; i < this.workers.length; i++) {
      let θ = (2 * Math.PI * i) / this.workers.length;
      let r = 40;
      let h = 50;
      let k = 50;
      let x = h + r * Math.cos(θ);
      let y = k + r * Math.sin(θ);
      anime({
        targets: "#" + this.workers[i],
        r: workerIdleSize,
        cx: x + "%",
        cy: y + "%",
        easing: "easeInOutQuint",
        duration: 500
      });
    }
  }

  run_task(worker_id, task_name, duration, color) {
    let worker = document.getElementById(worker_id);
    let scheduler = document.getElementById("scheduler");
    let arc = this.draw_arc(scheduler, worker, color, "projectile");

    anime
      .timeline({
        targets: "#" + worker_id
      })
      .add({
        begin: () => this.dashboard.insertBefore(arc, this.schedulerNode)
      })
      .add(
        {
          fill: color,
          r: workerBusySize,
          begin: () => this.dashboard.removeChild(arc)
        },
        500
      )
      .add({ fill: workerColor, r: workerIdleSize }, "+=" + duration);
  }

  run_transfer(start_worker, end_worker, duration) {
    start_worker = document.getElementById(start_worker);
    end_worker = document.getElementById(end_worker);
    duration = Math.max(250, duration / 1000);
    let color = "rgba(255, 0, 0, .6)";
    let arc = this.draw_arc(start_worker, end_worker, color, "transfer");

    anime
      .timeline({
        targets: ["#" + start_worker, "#" + end_worker],
        duration: 250
      })
      .add({
        fill: color,
        r: workerBusySize,
        begin: () => this.dashboard.insertBefore(arc, this.schedulerNode)
      })
      .add(
        {
          fill: workerColor,
          r: workerIdleSize,
          begin: () => this.dashboard.removeChild(arc)
        },
        "+=" + duration
      );
  }

  run_swap(worker, duration) {
    anime
      .timeline({
        targets: "#" + worker,
        duration: 250
      })
      .add({
        fill: "#D67548",
        r: workerBusySize
      })
      .add(
        {
          fill: workerColor,
          r: workerIdleSize
        },
        "+=" + duration
      );
  }

  run_deserialize(worker, duration) {
    anime
      .timeline({
        targets: "#" + worker,
        duration: 250
      })
      .add({
        fill: "gray",
        r: workerBusySize
      })
      .add(
        {
          fill: workerColor,
          r: workerIdleSize
        },
        "+=" + duration
      );
  }

  kill_worker(worker) {
    anime({
      targets: "#" + worker,
      fill: "rgba(0, 0, 0, 1)",
      duration: 250
    });
  }

  reset() {
    for (var i = 0; i < this.workers.length; i++) {
      anime({
        targets: "#" + this.workers[i],
        fill: workerColor,
        r: workerIdleSize,
        duration: 250
      });
    }
  }

  calculate_arc(start_x, start_y, end_x, end_y) {
    // mid-point of line:
    let mpx = (start_x + end_x) * 0.5;
    let mpy = (start_y + end_y) * 0.5;

    // angle of perpendicular to line:
    let theta = Math.atan2(start_y - end_y, start_x - end_x) - Math.PI / 2;

    // distance of control point from mid-point of line:
    let offset = Math.random() * 50;
    if (Math.random() >= 0.5) {
      offset = -offset;
    }

    // location of control point:
    let c1x = mpx + offset * Math.cos(theta);
    let c1y = mpy + offset * Math.sin(theta);

    // construct the command to draw a quadratic curve
    return (
      "M" +
      end_x +
      " " +
      end_y +
      " Q " +
      c1x +
      " " +
      c1y +
      " " +
      start_x +
      " " +
      start_y
    );
  }

  draw_arc(start_element, end_element, color, class_name) {
    let curve = this.calculate_arc(
      this.getAbsoluteXY(start_element)[0],
      this.getAbsoluteXY(start_element)[1],
      this.getAbsoluteXY(end_element)[0],
      this.getAbsoluteXY(end_element)[1]
    );

    let arc = document.createElementNS("http://www.w3.org/2000/svg", "path");
    arc.setAttributeNS(null, "id", class_name);
    arc.setAttributeNS(null, "class", class_name);
    arc.setAttributeNS(null, "stroke", color);
    arc.setAttribute("d", curve);
    return arc;
  }

  getAbsoluteXY(element) {
    var box = element.getBoundingClientRect();
    var x = box.left + box.width / 4;
    var y = box.top + box.height / 4;
    return [x, y];
  }
}

function get_websocket_url(endpoint) {
  var l = document.location;
  return (
    (l.protocol === "https:" ? "wss://" : "ws://") +
    l.hostname +
    (l.port != 80 && l.port != 443 ? ":" + l.port : "") +
    l.pathname.replace("/statics/individual-cluster-map.html", endpoint)
  );
}

function main() {
  dashboard = new Dashboard();

  var ws = new ReconnectingWebSocket(get_websocket_url("/eventstream"));
  ws.onopen = function() {
    dashboard.connected();
  };
  ws.onmessage = function(event) {
    dashboard.handle_event(JSON.parse(event.data));
  };
  ws.onclose = function() {
    dashboard.disconnected();
  };
}

window.addEventListener("load", main);
