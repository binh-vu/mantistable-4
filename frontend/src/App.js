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

function TablesPresentationHeader(props) {
    const total = props.total;
    const count = props.count;

    if (count < total) {
        return <p>I found {total} tables (showing {count})</p>
    } else {
        return <p>I found {total} tables</p>
    }
}

function TablesPresentation(props) {
    const tables = props.tables;
    const count = props.count;

    if (tables.length > 0) {
        return (
            <React.Fragment>
                <TablesPresentationHeader count={tables.length} total={count}/>
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
        return (
            <div>
                <hr/>
                <p>No tables here :(</p>
                <p>Go to dashboard and import some json tables</p>
                <hr/>
            </div>
        );
    }
}

class App extends React.Component {
    constructor(props) {
        super(props);
        this.state = {
            tables: [],
            count: 0
        }
    }
    
    componentDidMount() {
        // Fetch simple api endpoint
        fetch("http://web:8000/webapi/tables/")
        .then(res => res.json())
        .then(
            (data) => {
                this.setState({
                    tables: data.results,
                    count: data.count
                });
                console.log(data)
            },
            (error) => {
                this.setState({
                    tables: [],      // Just an empty result...
                    count: 0
                });
                console.log(error)
            }
        );
    }
    
    render() {
        return (
            <div className="App-header">
                <p>Hello, this is a React test page</p>
                <img alt="" className="App-logo" src="logo192.png"></img>
                <TablesPresentation tables={this.state.tables} count={this.state.count}/>
            </div>
        )
    }
}

export default App;
