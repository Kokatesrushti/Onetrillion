import React from 'react'
import {Routes,Route} from 'react-router-dom';
import Login from './Pages/Login';
import Home from './Pages/Home';
import Upload from './Pages/Upload';
import Dashboard from './Pages/Dashboard';
import Export from './Pages/Export';
import Campign from './Pages/Campign';
function AllRoutes() {
  return (
    <Routes >
        <Route path='/' element = {<Home/>} />
        <Route path='/login' element={<Login/>} />
        <Route path='/dashboard' element={<Dashboard />} />
        <Route path='/upload' element={<Upload />} />
        <Route path='/export' element={<Export />} />
        <Route path='/campign' element={<Campign/>} />
    </Routes>
  )
}

export default AllRoutes