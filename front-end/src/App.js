import './App.css';
import { connect } from 'react-redux';
import { simpleAction } from './actions/simpleAction';

import React, { Component } from 'react'
import Test2 from './components/Test';
import AccessTrend from './components/AccessTrend';
import Introduction from './components/Introduction';
import RealTimeAccess from './components/RealTimeAccess';


class App extends Component {

  simpleAction = (event) => {
    this.props.simpleAction();
   }
  render() {
    return (
      <div class="ui container">
          <Introduction />
          {/* <button onClick={this.simpleAction}>Test redux action</button> */}
          <RealTimeAccess />
          <AccessTrend />
      </div>
    );
  }
}

const mapStateToProps = state => ({
  ...state
 })
 const mapDispatchToProps = dispatch => ({
  simpleAction: () => dispatch(simpleAction())
 })
 export default connect(mapStateToProps, mapDispatchToProps)(App);
