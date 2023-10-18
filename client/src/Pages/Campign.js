import React, { useState } from "react";

function Campign() {
  const [isSwitchOn, setIsSwitchOn] = useState(false);
  const [numWords, setNumWords] = useState(0);
  const [bidAggression, setBidAggression] = useState(50);

  const handleToggleChange = () => {
    setIsSwitchOn(!isSwitchOn);
  };

  const handleNumWordsChange = (e) => {
    const value = parseFloat(e.target.value);
    setNumWords(value);
  };

  const handleBidAggressionChange = (e) => {
    const value = parseFloat(e.target.value);
    setBidAggression(value);
  };

  return (
    <div className="flex flex-grow mt-2">
      <div className="flex flex-col w-full p-6 bg-white rounded-lg shadow-lg shadow-stone-400 m-10">
        <div className="flex space-x-2">
          <p>Add new optimal keywords</p>
          <div className="flex items-center">
            <input
              className="mr-2 mt-[0.3rem] h-3.5 w-8 appearance-none rounded-[0.4375rem] bg-neutral-300 before:pointer-events-none before:absolute before:h-3.5 before:w-3.5 before:rounded-full before:bg-transparent before:content-[''] after:absolute after:z-[2] after:-mt-[0.1875rem] after:h-5 after:w-5 after:rounded-full after:border-none after:bg-neutral-100 after:shadow-[0 0px 3px 0 rgba(0,0,0,0.7), 0 2px 2px 0 rgba(0,0,0,0.4)] after:transition-[background-color 0.2s, transform 0.2s] after:content-[''] checked:bg-primary checked:after:absolute checked:after:z-[2] checked:after:-mt-[3px] checked:after:ml-[1.0625rem] checked:after:h-5 checked:after:w-5 checked:after:rounded-full checked:after:border-none checked:after:bg-primary checked:after:shadow-[0 3px 1px -2px rgba(0,0,0,0.2), 0 2px 2px 0 rgba(0,0,0,0.14), 0 1px 5px 0 rgba(0,0,0,0.12)] checked:after:transition-[background-color 0.2s, transform 0.2s] checked:after:content-[''] hover:cursor-pointer focus:outline-none focus:ring-0 focus:before:scale-100 focus:before:opacity-[0.12] focus:before:shadow-[3px -1px 0px 13px rgba(0,0,0,0.6)] focus:before:transition-[box-shadow 0.2s, transform 0.2s] focus:after:absolute focus:after:z-[1] focus:after:block focus:after:h-5 focus:after:w-5 focus:after:rounded-full focus:after:content-[''] checked:focus:border-primary checked:focus:bg-primary checked:focus:before:ml-[1.0625rem] checked:focus:before:scale-100 checked:focus:before:shadow-[3px -1px 0px 13px #3b71ca] checked:focus:before:transition-[box-shadow 0.2s, transform 0.2s] dark:bg-blue-600 dark:after:bg-white-400 dark:checked:bg-primary dark:checked:after:bg-primary dark:focus:before:shadow-[3px -1px 0px 13px rgba(255,255,255,0.4)] dark:checked:focus:before:shadow-[3px -1px 0px 13px #3b71ca]"
              type="checkbox"
              role="switch"
              id="flexSwitchCheckDefault"
              checked={isSwitchOn}
              onChange={handleToggleChange}
            />
          </div>
        </div>
        {isSwitchOn && (
          <div className="mt-4">
            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="numWordsSlider">No. of Words</label>
              <div className="ml-20">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>
            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="BidAggression"> No. of Keywords Bid Aggression</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>
         
          </div>
        )}


        <hr />

        <div className="flex space-x-2 mt-4">
          <p>Add new optimal keywords</p>
          <div className="flex items-center">
            <input
              className="mr-2 mt-[0.3rem] h-3.5 w-8 appearance-none rounded-[0.4375rem] bg-neutral-300 before:pointer-events-none before:absolute before:h-3.5 before:w-3.5 before:rounded-full before:bg-transparent before:content-[''] after:absolute after:z-[2] after:-mt-[0.1875rem] after:h-5 after:w-5 after:rounded-full after:border-none after:bg-neutral-100 after:shadow-[0 0px 3px 0 rgba(0,0,0,0.7), 0 2px 2px 0 rgba(0,0,0,0.4)] after:transition-[background-color 0.2s, transform 0.2s] after:content-[''] checked:bg-primary checked:after:absolute checked:after:z-[2] checked:after:-mt-[3px] checked:after:ml-[1.0625rem] checked:after:h-5 checked:after:w-5 checked:after:rounded-full checked:after:border-none checked:after:bg-primary checked:after:shadow-[0 3px 1px -2px rgba(0,0,0,0.2), 0 2px 2px 0 rgba(0,0,0,0.14), 0 1px 5px 0 rgba(0,0,0,0.12)] checked:after:transition-[background-color 0.2s, transform 0.2s] checked:after:content-[''] hover:cursor-pointer focus:outline-none focus:ring-0 focus:before:scale-100 focus:before:opacity-[0.12] focus:before:shadow-[3px -1px 0px 13px rgba(0,0,0,0.6)] focus:before:transition-[box-shadow 0.2s, transform 0.2s] focus:after:absolute focus:after:z-[1] focus:after:block focus:after:h-5 focus:after:w-5 focus:after:rounded-full focus:after:content-[''] checked:focus:border-primary checked:focus:bg-primary checked:focus:before:ml-[1.0625rem] checked:focus:before:scale-100 checked:focus:before:shadow-[3px -1px 0px 13px #3b71ca] checked:focus:before:transition-[box-shadow 0.2s, transform 0.2s] dark:bg-neutral-600 dark:after:bg-neutral-400 dark:checked:bg-primary dark:checked:after:bg-primary dark:focus:before:shadow-[3px -1px 0px 13px rgba(255,255,255,0.4)] dark:checked:focus:before:shadow-[3px -1px 0px 13px #3b71ca]"
              type="checkbox"
              role="switch"
              id="flexSwitchCheckDefault"
              checked={isSwitchOn}
              onChange={handleToggleChange}
            />
          </div>
        </div>
        {isSwitchOn && (
          <div className="mt-4">
            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="numWordsSlider">Top Customer Search Terms</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>
            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="BidAggression"> No. of Keywords Bid Aggression</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>

            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="BidAggression">Boost Bids on Peak Days</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>
            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="BidAggression"> Lowers Bids on Peak Days</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>


            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="BidAggression">Lowers Bids on slow Days</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>

            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="BidAggression">Boost bids during peak hours</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>

            <div className="mb-3 flex flex-row items-center">
              
              <label htmlFor="BidAggression">Lower Bids During Off peak hours</label>
              <div className="ml-12">
              <p className="text-left ml-12">{numWords.toFixed(0)}%</p>
              <input
                type="range"
                id="numWordsSlider"
                min="0"
                max="100"
                value={numWords}
                onChange={handleNumWordsChange}
                className="slider"
              />
              </div>
             
            </div>
         
          </div>
        )}
        
      </div>
    </div>
  );
}

export default Campign;