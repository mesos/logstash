"use strict";

let Link = ReactRouter.Link;

let RestClient = {
    get(url, callback) {
        var request = new XMLHttpRequest();
        request.open('GET', url, true);

        request.onload = function () {
            if (request.status >= 200 && request.status < 400) {
                var data = JSON.parse(request.responseText);
                callback(null, data);
            } else {
                callback("An error occurred");
            }
        };

        request.onerror = function () {
            callback("An error occurred");
        };

        request.send();
    },
    doDelete(url, callback){
        var request = new XMLHttpRequest();   // new HttpRequest instance
        request.open("DELETE", url);


        request.onload = function () {
            if (request.status >= 200 && request.status < 400) {
                callback(null);
            } else {
                callback("An error occurred");
            }
        };

        request.onerror = function () {
            callback("An error occurred");
        };

        request.send();

    },
    put(url, data, callback) {
        var request = new XMLHttpRequest();   // new HttpRequest instance
        request.open("PUT", url);
        request.setRequestHeader("Content-Type", "application/json;charset=UTF-8");


        request.onload = function () {
            if (request.status >= 200 && request.status < 400) {
                var data = JSON.parse(request.responseText);
                callback(null, data);
            } else {
                callback("An error occurred");
            }
        };

        request.onerror = function () {
            callback("An error occurred");
        };

        request.send(JSON.stringify(data));
    },

    post(url, data, callback) {
        var request = new XMLHttpRequest();   // new HttpRequest instance
        request.open("POST", url);
        request.setRequestHeader("Content-Type", "application/json;charset=UTF-8");


        request.onload = function () {
            if (request.status >= 200 && request.status < 400) {
                var data = JSON.parse(request.responseText);
                callback(null, data);
            } else {
                callback("An error occurred");
            }
        };

        request.onerror = function () {
            callback("An error occurred");
        };

        request.send(JSON.stringify(data));
    }
};

let SplitPage = React.createClass({
    render() {
        return (
          <div className="splitPage">
              <div className="splitPage__left">
                  {this.props.leftContent}
              </div>
              <div className="splitPage__right">
                  {this.props.rightContent}
              </div>
          </div>
        )
    }
});

let WebSocketClient = function (topics) {
    return {
        componentWillMount() {
            let component = this;
            let socket = new SockJS('/ws');
            let stompClient = Stomp.over(socket);
            stompClient.debug = null;
            this.setState({stompClient: stompClient});

            stompClient.connect({}, function (frame) {
                for (let i in topics) {
                    if (topics.hasOwnProperty(i)) {
                        let topic = topics[i];
                        stompClient.subscribe('/topic/' + topic, function (message) {
                            let content = JSON.parse(message.body);
                            if (component.webSocketData) component.webSocketData(content, topic);
                        });
                    }
                }
            });
        },

        componentWillUnmount() {
            this.state.stompClient.disconnect();
            console.log("Disconnected");
        }
    };
};

let Icon = React.createClass({
    render() {
        let style = this.props.color ? {color: this.props.color || "inherit"} : null;
        return <span style={style}
                     className={"icon-" + this.props.name + " " + this.props.className}></span>;
    }
});

let Menu = React.createClass({
    render() {
        return (
          <div className="menu">
              <div>
                  <div className="menu__header">
                      <img src="logstash_icon.svg" className="menu__logo"/>

                      <div>Mesos Logstash</div>
                  </div>
                  <div className="menu__list">
                      <Link className="menu__list-item" activeClassName="menu__list-item--active"
                            to="dashboard"><Icon className="menu__icon" name="dashboard"/>Dashboard</Link>
                      <Link className="menu__list-item" activeClassName="menu__list-item--active"
                            to="nodes"><Icon className="menu__icon" name="nodes"/>Nodes</Link>
                      <Link className="menu__list-item" activeClassName="menu__list-item--active"
                            to="config"><Icon className="menu__icon" name="config"/>Config</Link>
                  </div>
              </div>
              <div className="menu__footer">
                  <a className="menu__footer-item"
                     href="http://github.com/mesos/logstash">
                      GitHub
                  </a>
                  <a className="menu__footer-item"
                     href="http://github.com/mesos/logstash">
                      Documentation
                  </a>
              </div>
          </div>);
    }
});

let Chart = React.createClass({

    getInitialState() {
        return {
            chart: null,
            latestX: null,
        }
    },

    roundT(t) {
        return Math.floor(t / 1000) * 1000;
    },

    componentWillReceiveProps(nextProps) {
        let chart = this.state.chart;
        let series = chart.series[0];
        let latestX = this.state.latestX;
        let x = this.roundT(new Date().getTime());
        if (x !== latestX) {

            let y = nextProps.value;
            chart.xAxis[0].setExtremes(x - 60 * 1000, x);

            var l = series.points.length;
            var p = series.points[l - 1];
            p.update({marker: {enabled: false}});

            series.addPoint({
                x: x, y: y, marker: {
                    enabled: true,
                    symbol: 'circle',
                    radius: 3
                }
            }, true, true);

            this.setState({latestX: x});
        }
    },

    componentDidMount() {
        let el = this.getDOMNode();
        let now = this.roundT(new Date().getTime());
        let self = this;
        let chart = new Highcharts.Chart({
            chart: {
                type: "area",
                renderTo: el,
                backgroundColor: "transparent",
                height: 120,
                width: 286
            },

            legend: {
                enabled: false
            },

            colors: [this.props.color || "#00FF00"],

            credits: {
                enabled: false
            },

            title: {
                text: null
            },

            plotOptions: {
                area: {
                    lineWidth: 1,
                    fillOpacity: .25,
                }
            },

            yAxis: {
                tickInterval: 1,
                min: 0,
                lineColor: "transparent",
                gridLineColor: "#2E3447",
                title: {
                    text: null
                },
                labels: {
                    style: {color: "#5E626D"}
                }
            },

            xAxis: {
                type: 'datetime',
                lineColor: "#2E3447",
                tickLength: 0,
                endOnTick: false,
                startOnTick: false,
                title: {
                    text: null
                },
                labels: {
                    enabled: false
                },
                alternateGridColor: "#2E3447",
                tickPixelInterval: 20,
            },

            series: [{
                name: "Count",
                cropThreshold: 100,
                pointStart: now - 60000,
                pointInterval: 1000,
                data: (function () {
                    let data = [];
                    let count = 100;
                    for (let i = 0; i < count; i++) {
                        let t = self.roundT(now - (count - i) * 1000);
                        data.push({x: t, y: 0});
                    }
                    return data;
                }())
            }]

        });

        this.setState({chart: chart});
    },

    render() {
        return <div></div>
    }
});


let NodePage = React.createClass({
    mixins: [
        WebSocketClient(["nodes"])
    ],

    getInitialState() {
        return {
            tasks: []
        };
    },

    webSocketData(data) {
        this.setState({tasks: data.tasks});
    },

    render() {
        let renderItem = function (k, v) {
            return (
              <li className="box-list__item">
                  <div className="box-list__key">{k}</div>
                  <div className="box-list__value">{v}</div>
              </li>
            );
        };

        let formatName = function (c) {
            let text = c.imageName + " (" + c.containerId.substr(0, 8) + ") ";
            return text;
        };

        let statusIcon = function (status) {
            switch (status) {
                case "STREAMING":
                    return <div title="Logging Enabled" className="status-icon status--healthy"></div>;
                default:
                    return <div title="Not Logging" className="status-icon status--idle"></div>;
            }
        };

        let healthText = function (status) {
            if (status === "RUNNING") {
                return <div className="status status--healthy">Healthy</div>
            }
            else if (status === "ERROR") {
                return <div className="status status--error">Sick</div>
            }
            else {
                return <div className="status status--idle">{status}</div>
            }
        };

        let renderTask = function (t) {
            return (
              <div className="box box--list">
                  <div className="box__header">
                      <div>{t.hostName}</div>
                      <div className="status status--healthy">{healthText(t.status)}</div>
                  </div>
                  <div className="box__body">
                      <ul className="box-list">
                          {renderItem("Task ID", t.taskId)}
                          {renderItem("Slave ID", t.slaveId)}
                          {renderItem("Executor ID", t.executorId)}
                          {renderItem("", "")}
                          {renderItem("Discovered Docker Containers", "("+t.containers.length+")")}
                          {renderItem("", "")}
                          {t.containers.map(function(c) {
                              return renderItem(formatName(c), statusIcon(c.status));
                          })}
                      </ul>
                  </div>
              </div>);
        };

        return (
          <div className="page">
              <div>{this.state.tasks.map(renderTask)}</div>
          </div>);
    }
});


let ConfigPage = React.createClass({
    getInitialState() {
        return {
            configs: [],
            hostConfig: null
        };
    },

    componentWillMount() {
        let component = this;

        RestClient.get("/api/configs", function (error, data) {
            if (error) {
                console.error(error);
                return;
            }

            component.setState({configs: data});
        });

        RestClient.get("/api/host-config", function (error, data) {
            if (error) {
                console.error(error);
                return;
            }
            component.setState({hostConfig: data.input || ""});
        });
    },

    onCreated(c) {
        let newConfigs = [].concat(this.state.configs);
        newConfigs.push(c);
        this.setState({configs: newConfigs});
    },

    onDeleted(c) {
        let newConfigs = [];

        for (var i = 0; i < this.state.configs.length; i ++){
            if (c.name !== this.state.configs[i].name){
                newConfigs.push(this.state.configs[i]);
            }
        }
        this.setState({configs: newConfigs});
    },

    render() {
        let self = this;
        let configs = this.state.configs;


        return (
          <div className="page">
              <h1>Configurations</h1>

              <h2>Slave Configuration</h2>
              <div>Slave configurations will be propagated to all running logstash-instances. They are a good place to put output-blocks and to specify slave log-files to be monitored.</div>
              <div>
                  You can use the special key 'host-path' to configure file-plugins inside logstash input-blocks to indicate that a file should be logged from the slave itself (instead of the executor's docker container).
                  <div>
                      Example:
                      <pre>
                          input &#123;
                          file &#123;
                          "host-path" =&gt; "/var/log/hello.log"
                          &#125;
                          &#125;
                       </pre>
                  </div>
                  This will configure logstash to monitor the file <code>/var/log/hello.log</code> on all slaves. Note: this requires that the logstash framework is configured to use <code>/var/log</code> as a volume.
              </div>
              <br />
              {self.state.hostConfig === null ? "Loading..." :
                <form action="/api/host-config" method="POST">
                    <input type="hidden" name="_method" value="PUT"/>
                    <input type="hidden" className="configForm__name" name="name" value="ui"/>
                        <textarea className="configForm__input" name="input"
                                  placeholder="Slave Config"
                                  defaultValue={self.state.hostConfig}></textarea>
                    <br />
                    <button type="submit">Update</button>
                </form>
              }
              <h2>Docker Container Configurations</h2>
              <div>
                  <div>
                      Docker configurations will be propagated to slaves running docker containers with image names matching the docker configuration. Note that tags are usually included in docker image names.
                  </div>
                  <div>
                      Example: a slave running a docker container with the <code>nginx:latest</code> docker image will be provided a docker configuration with 'applicable image name' set to <code>nginx:latest</code>, if it exists.
                  </div>
              </div>
              <div>
                  <div>
                      You can use the special key 'docker-path' to configure file-plugins inside logstash input-blocks to indicate that a file should be logged from within the docker container itself.
                  </div>
                  <div>
                      Example:
                      <pre>
                          input &#123;
                          file &#123;
                          "docker-path" =&gt; "/var/log/hello.log"
                          &#125;
                          &#125;
                       </pre>
                      This will configure logstash to monitor the file <code>/var/log/hello.log</code> <emph>inside</emph> all applicable docker containers.
                  </div>
              </div>
              {configs === null ? "Loading..." : (
                <ul className="configList">
                    {configs.map(function(c) { return <Config config={c} onDeleted={self.onDeleted} /> })}
                </ul>)
              }

              <h2>New Docker Configuration</h2>

              <Config onCreated={this.onCreated} />
          </div>);
    }
});


let Config = React.createClass({

    update() {
        let urlEncodedName = encodeURIComponent(this.props.config.name);
        let url = "/api/configs?name=" + urlEncodedName;

        let data = {
            input: this.refs.config.getDOMNode().value
        };

        RestClient.put(url, data, function(response, data) {
            if (response == null){
                console.log(data);
            } else {
                console.log(response);
            }
        });
    },

    remove() {
        let self = this;
        let config = this.props.config;
        let urlEncodedName = encodeURIComponent(config.name);
        let url = "/api/configs?name=" + urlEncodedName;


        RestClient.doDelete(url, function(response) {
            if (response == null){
                self.props.onDeleted(config);
            } else {
                console.log(response);
            }
        });
    },

    create() {
        let self = this;
        let name = self.refs.name.getDOMNode().value;
        let url = "/api/configs";

        if (name == null || name ===""){
            console.log("Name must not be empty");
            return;
        }

        let data = {
            input: self.refs.config.getDOMNode().value,
            name : name
        };

        RestClient.post(url, data, function(response, data) {
            if (response == null){
                self.props.onCreated(data);
                self.refs.name.getDOMNode().value = "";
                self.refs.config.getDOMNode().value = "";

            } else {
                console.log(response);
            }
        });
    },

    render() {
        let self = this;
        let c = this.props.config;

        let renderExisting = function() {
            return (
              <li className="config">
                  <h3>{c.name}</h3>
                  <textarea ref="config"
                            className="configForm__input"
                            placeholder="Logstash Config (Input And Filter Only)"
                            defaultValue={c.input}></textarea>
                  <br />
                  <button type="button" onClick={self.update}>Update</button>
                  <br />
                  <button type="button" onClick={self.remove}>Delete</button>
              </li>);
        };

        let renderNew = function() {
            return (
              <div><input className="configForm__name" ref="name" name="name" placeholder="applicable to docker containers with image name (tag included). E.g: nginx:latest"/>
                  <br />
                  <textarea className="configForm__input" ref="config" name="input"
                            placeholder='Logstash Config (Input And Filter Only). E.g, input { file { "docker-path" => "/var/log/hello.log" } }'></textarea>
                  <br />
                  <button type="button" onClick={self.create}>Create</button>
              </div>);
        };

        return ( c ? renderExisting() : renderNew());
    }
});


let Box = React.createClass({
    render() {
        return (
          <div className="box">
              <div className="box__header">{this.props.title}</div>
              <div className="box__body">
                  <div>{this.props.children}</div>
                  {this.props.subtitle ?
                    <div className="box__subtitle">{this.props.subtitle}</div> : ""}
              </div>
          </div>
        );
    }
});

let BigNumber = React.createClass({
    render() {
        return (
          <div className="big-number">
              {this.props.value}
              <div className="big-number__title" style={{color: this.props.color || "white"}}>
                  {this.props.title}
              </div>
          </div>
        );
    }
});


let DashboardPage = React.createClass({
    mixins: [
        WebSocketClient(["stats", "nodes"])
    ],

    getInitialState() {
        return {
            connected: false,
            client: null,
            stats: null,
            nodes: null
        };
    },

    webSocketData(data, topic) {
        let state = {};
        state[topic] = data;
        this.setState(state);
    },

    render() {
        let stats = this.state.stats;
        let nodes = this.state.nodes;
        if (!stats || !nodes) return <div>Connecting...</div>;

        let streamTotal = nodes.tasks.reduce(function (acc, t) {
            return acc + t.activeStreamCount;
        }, 0);

        return (
          <div className="page">
              <Box title="Running Logstash Executors" subtitle="Last 60 seconds">
                  <BigNumber value={nodes.tasks.length} title="Number of Nodes"
                             color="#E0ECF8"/>
                  <Chart value={nodes.tasks.length} color="#E0ECF8"/>
              </Box>

              <Box title="Observing Docker Containers" subtitle="Last 60 seconds">
                  <BigNumber value={streamTotal} title="Container File Logging" color="#E0ECF8"/>
                  <Chart value={streamTotal} color="#E0ECF8"/>
              </Box>
          </div>
        )
    }
});

let Route = ReactRouter.Route;
let DefaultRoute = ReactRouter.DefaultRoute;
let RouteHandler = ReactRouter.RouteHandler;

let App = React.createClass({
    render() {
        return (
          <div className="container">
              <img className="mesos-logo" src="mesos_logo.png"/>
              <SplitPage leftContent={<Menu />} rightContent={<RouteHandler />}/>
          </div>
        );
    }
});

let routes = (
  <Route path="/" handler={App}>
      <DefaultRoute handler={DashboardPage}/>
      <Route name="dashboard" handler={DashboardPage}/>
      <Route name="config" handler={ConfigPage}/>
      <Route name="nodes" handler={NodePage}/>
  </Route>
);


let AppRouter = ReactRouter.create({
    routes: routes,
    location: ReactRouter.HistoryLocation
});
