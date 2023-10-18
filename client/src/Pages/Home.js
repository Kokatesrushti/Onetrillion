import React, { useState, useEffect } from 'react';
import backgroundImage from '../background.jpg';

function Home() {
  const fullText = "Discover the Extraordinary";
  const [text, setText] = useState('');
  const [currentIndex, setCurrentIndex] = useState(0);

  useEffect(() => {
    const interval = setInterval(() => {
      if (currentIndex <= fullText.length) {
        setText(fullText.substring(0, currentIndex));
        setCurrentIndex((prevIndex) => (prevIndex + 1) % (fullText.length + 1));
      }
    },80);

    return () => clearInterval(interval);
  }, [currentIndex, fullText]);

  return (
    // <div
    //   className="bg-cover bg-center h-screen"
    //   style={{ backgroundImage: `url(${backgroundImage})` }}
    // >
    //   <div className="text-5xl text-left pt-40 pl-20 text-white">
    //     {text}
    //   </div>
    // </div>
    <div></div>
  );
}

export default Home;
