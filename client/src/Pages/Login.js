import React, { useState } from 'react';
import { useNavigate } from 'react-router-dom';

function Login() {
  const navigate = useNavigate();
  const [username, setUsername] = useState('');
  const [password, setPassword] = useState('');
  const [message, setMessage] = useState('');

  const handleLogin = async () => {
    try {
      const response = await fetch('http://localhost:5000/login', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({ username, password }),
      });

      if (response.status === 200) {
        setMessage('Login successful');
        navigate('/upload');
      } else {
        setMessage('Login failed');
      }
    } catch (error) {
      console.error('Error:', error);
      setMessage('Login failed');
    }
  };

  return (
    <div className="flex items-center justify-center bg-gray-50 flex-grow ">
      <div className="flex flex-col max-w-md w-full p-6 bg-white rounded-lg shadow-lg mb-5">
        <h1 className="text-lg text-center mb-10 font-semibold">User Authentication</h1>
        <label className="font-semibold mb-2">Username *</label>
        <input
          type="text"
          placeholder="Username"
          value={username}
          onChange={(e) => setUsername(e.target.value)}
          required
          className="w-full px-3 py-2 rounded-md focus:outline-none focus:ring focus:ring-gray-700 mb-5"
        />
        <label className="font-semibold mb-2">Password *</label>
        <input
          type="password"
          placeholder="Password"
          value={password}
          onChange={(e) => setPassword(e.target.value)}
          required
          className="w-full px-3 py-2 rounded-md focus:outline-none focus:ring focus:ring-gray-700 mb-5"
        />
        <button
          onClick={handleLogin}
          className="text-white py-2 px-4 rounded-md focus:outline-none bg-gradient-to-r from-purple-500 to-pink-500"
        >
          Login
        </button>
        <p>{message}</p>
      </div>
    </div>
  );
}

export default Login;
