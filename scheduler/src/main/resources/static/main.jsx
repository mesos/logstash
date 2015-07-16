"use strict";

let Link = ReactRouter.Link;

let RestClient = {
    get(url, callback) {
        var request = new XMLHttpRequest();
        request.open('GET', url, true);

        request.onload = function() {
            if (request.status >= 200 && request.status < 400) {
                var data = JSON.parse(request.responseText);
                callback(null, data);
            } else {
                callback("An error occurred");
            }
        };

        request.onerror = function() {
            callback("An error occurred");
        };

        request.send();
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


let WebSocketClient = function(topic) {
    return {
        componentWillMount() {
            let component = this;
            let socket = new SockJS('/ws');
            let stompClient = Stomp.over(socket);
            this.setState({stompClient: stompClient});

            stompClient.connect({}, function(frame) {
                stompClient.subscribe('/topic/' + topic, function(message) {
                    let content = JSON.parse(message.body);
                    if (component.webSocketData) component.webSocketData(content);
                });
            });
        },

        componentWillUnmount() {
            this.state.stompClient.disconnect();
            console.log("Disconnected");
        }
    };
};

let Menu = React.createClass({
   render() {
       return (
           <div className="menu">
           <div>
               <div className="menu__header">
                   Mesos Logstash
               </div>
                <div className="menu__list">
                    <Link className="menu__list-item" to="dashboard">Dashboard</Link>
                    <Link className="menu__list-item" to="nodes">Nodes</Link>
                    <Link className="menu__list-item" to="config">Config</Link>
                </div>
           </div>
           <div className="menu__footer">
               <a className="menu__footer-item" href="http://github.com/triforkse/logstash-mesos">
                   GitHub
               </a>
               <a className="menu__footer-item" href="http://github.com/triforkse/logstash-mesos">
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
          data: null
      }
    },

    componentWillReceiveProps(nextProps) {
        this.setState({data: [nextProps.value, 10, 30]});
    },

    componentDidMount() {
        let el = this.getDOMNode();
        let chart = new Highcharts.Chart({
            chart: {
                renderTo: el
            },

            xAxis: {
                categories: ['Jan', 'Feb', 'Mar']
            },

            series: [{
                data: [29.9, 10, 30]
            }]

        });

        this.setState({chart: chart});
    },

    componentWillUpdate(nextProps, nextState) {
        nextState.chart.series[0].setData(nextState.data);
    },

   render() {
       return <div></div>
   }
});


let NodePage = React.createClass({
    mixins: [
        WebSocketClient("nodes")
    ],

    getInitialState() {
      return {
          executors: []
      };
    },

    webSocketData(data) {
        console.log(data);
      this.setState({executors: data.executors});
    },

    render() {
        let renderRow = function(e) { return <tr><td>{e.slaveId}</td><td>{e.executorId}</td></tr>; };

        return (
            <div>
                <h1>Nodes</h1>
                <div>
                    <table>
                        <thead>
                            <th>Slave ID</th>
                            <th>Executor ID</th>
                        </thead>
                        {this.state.executors.map(renderRow)}
                    </table>
                </div>
            </div>);
    }
});


let ConfigPage = React.createClass({
    getInitialState() {
        return {
            configs: []
        };
    },

    componentWillMount() {
        let component = this;
        RestClient.get("/api/configs", function(error, data) {
            if (error) {
                console.error(error);
                return;
            }

            component.setState({configs: data});
        })
    },

    render() {
        let configs = this.state.configs;
        let renderConfig = function(c) {
            return (
                <li className="config">
                    <form action={"/api/configs/" + c.name} method="POST">
                        <input type="hidden" name="_method" value="PUT"/>
                        <input type="hidden" className="configForm__name" name="name" value={c.name} />
                        <h3>{c.name}</h3>
                        <textarea className="configForm__input" name="input" placeholder="Input Logstash Config">{c.input}</textarea>
                        <br />
                        <button type="submit">Update</button>
                    </form>
                </li>
            );
        };
        return (
            <div className="page">
                <h1>Config</h1>
                <div>
                    <ul className="configList">
                        {configs.map(renderConfig)}
                    </ul>
                </div>
                <div>
                    <h2>New Framework Config</h2>
                    <form className="configForm" action="/api/configs" method="POST">
                        <input className="configForm__name" name="name" placeholder="docker image name" /><br />
                        <textarea className="configForm__input" name="input" placeholder="Input Logstash Config"></textarea>
                        <br />
                        <button type="submit">Create</button>
                    </form>
                </div>
            </div>);
    }
});


let DashboardPage = React.createClass({
    mixins: [
        WebSocketClient("stats")
    ],

    getInitialState() {
        return {
            connected: false,
            client: null,
            stats: null
        };
    },

    webSocketData(data) {
        this.setState({stats: data});
    },

    render() {
        let stats = this.state.stats;
        if (!stats) return <div>Connecting...</div>;

        return (
            <div>
                <h1>Dashboard</h1>
                <div>
                    <div>CPUs: {stats.cpus}</div>
                    <div>Mem: {stats.mem}</div>
                    <div>Nodes: {stats.numNodes}</div>
                    <div>Disk: {stats.disk}</div>
                </div>
                <Chart value={stats.numNodes} />
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
                <img className="mesos-logo" src="mesos_logo.png" />
                <SplitPage leftContent={<Menu />} rightContent={<RouteHandler />} />
            </div>
        );
    }
});

let routes = (
    <Route path="/" handler={App}>
        <DefaultRoute handler={DashboardPage} />
        <Route name="dashboard" handler={DashboardPage} />
        <Route name="config" handler={ConfigPage} />
        <Route name="nodes" handler={NodePage} />
    </Route>
);


let AppRouter = ReactRouter.create({
    routes: routes,
    location: ReactRouter.HistoryLocation
});
