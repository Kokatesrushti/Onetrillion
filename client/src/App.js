import React from "react";
import { BrowserRouter as Router} from "react-router-dom";
import Navbar from "./components/Navbar";
import AllRoutes from "./AllRoutes";
function App() {
  // const [isLoggedIn, setIsLoggedIn] = useState(false);
  // const handleLogout = () => {
  //   setIsLoggedIn(false);
  // };
  // const handleLogin = () => {
  //   setIsLoggedIn(true);
  // };

  return (
  <div className="flex">
  <Router >
  <Navbar/>
  <AllRoutes/>
  </Router>
  </div>
 


    
  );
}

export default App;
