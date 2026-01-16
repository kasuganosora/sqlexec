function calculate_age(birthdate) {
    if (!birthdate) {
        return null;
    }
    
    const birth = new Date(birthdate);
    const today = new Date();
    
    if (isNaN(birth.getTime())) {
        return null;
    }
    
    let age = today.getFullYear() - birth.getFullYear();
    const monthDiff = today.getMonth() - birth.getMonth();
    
    if (monthDiff < 0 || (monthDiff === 0 && today.getDate() < birth.getDate())) {
        age--;
    }
    
    return age;
} 