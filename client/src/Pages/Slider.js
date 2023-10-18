import React, { Component } from 'react';
// import 'react-input-range/lib/css/index.css';
import 'tailwindcss/tailwind.css';

class Slider extends Component {
  constructor(props) {
    super(props);

    this.state = {
      value: 50, // Set the initial value here (between 0 and 100)
    };
  }

  handleChange = (value) => {
    this.setState({ value });
  };

  render() {
    return (
      <div className="w-1/2 mx-auto mt-8">
        <h2 className="text-2xl font-semibold mb-4">Slider Value: {this.state.value}</h2>
        <input
          type="range"
          min={0}
          max={100}
          value={this.state.value}
          onChange={(e) => this.handleChange(Number(e.target.value))}
          className="slider thumb-4 w-full"
        />
      </div>
    );
  }
}

export default Slider;