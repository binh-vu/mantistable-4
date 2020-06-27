import React from 'react';

import './App.css'

function TableComponent(props) {
    return (
        <div>
            <hr/>
            <p>Table name: {props.name}</p>
            <p>Dataset name: {props.dataset}</p>
            <p>Rows: {props.rows_count}</p>
            <p>Cols: {props.cols_count}</p>
            <p>Annotated: {props.has_annotations ? "yes" : "no"}</p>
        </div>
    );
}

function TablesPresentation(props) {
    const tables = props.tables;
    if (tables.length > 0) {
        let url = "http://" + window.location.hostname + ":8000/webapi/tables/";
        return (
            <React.Fragment>
                <p>I found {tables.length} <a className="App-link" href={url}>tables</a></p>
                {
                    tables.map((item) => {
                        return (
                            <TableComponent
                                key={item.name}
                                name={item.name}
                                dataset={item.dataset}
                                rows_count={item.rows_count}
                                cols_count={item.cols_count}
                                has_annotations={item.has_annotations}
                            />
                        )
                    })
                }
            </React.Fragment>
        );
    } else {
        let url = "http://" + window.location.hostname + ":8000";
        return (
            <div>
                <hr/>
                <p>No tables here :(</p>
                <p>Go <a className="App-link" href={url}>here</a> and import some json tables</p>
                <hr/>
            </div>
        );
    }
}

class App extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            tables: []
        }
    }
    
    componentDidMount() {
        // Fetch simple api endpoint
        fetch("/webapi/tables/")
        .then(res => res.json())
        .then(
            (result) => {
                this.setState({
                    tables: result
                });
            },
            (error) => {
                this.setState({
                    tables: []      // Just an empty result...
                });
            }
        );
    }
    
  render() {
    return (
        <div className="App-header">
            <p>Hello, this is a React test page</p>
            <img alt="" className="App-logo" src="logo192.png"></img>
            <TablesPresentation tables={this.state.tables} />
        </div>
    )
  }
}

export default App;
