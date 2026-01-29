import React from 'react';
import ReportPage from './components/ReportPage';
import { AuthProvider } from './components/AuthProvider';


const App: React.FC = () => {
  return (
    <AuthProvider>
      <div className="App">
        <ReportPage/>
      </div>
    </AuthProvider>
  );
};

export default App;