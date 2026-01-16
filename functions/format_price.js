function format_price(price, currency = 'CNY') {
    if (typeof price !== 'number') {
        price = parseFloat(price);
    }
    
    if (isNaN(price)) {
        return '';
    }
    
    const formatter = new Intl.NumberFormat('zh-CN', {
        style: 'currency',
        currency: currency,
        minimumFractionDigits: 2,
        maximumFractionDigits: 2
    });
    
    return formatter.format(price);
} 