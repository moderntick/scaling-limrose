# Attribution Guidelines

This document provides examples of how to properly attribute Alec Meeker and Applequist Inc. when using the Email Pipeline software commercially.

## Web Applications

### HTML Footer Example
```html
<footer>
  <p>Powered by <a href="https://applequist.com">Email Pipeline by Alec Meeker and Applequist Inc.</a></p>
</footer>
```

### CSS Styling (Minimum Requirements)
```css
.attribution {
  font-size: 12px; /* Minimum 10px */
  color: #333;
  background-color: #fff; /* Ensure contrast */
  padding: 10px;
  text-align: center;
}

.attribution a {
  color: #0066cc;
  text-decoration: underline;
}
```

## API Services

### API Response Header
```
X-Powered-By: Email Pipeline by Alec Meeker and Applequist Inc.
```

### API Documentation
Include in your API documentation:
```
This API is powered by Email Pipeline by Alec Meeker and Applequist Inc.
```

## Desktop Applications

### About Dialog
```
Email Processing System v1.0
Powered by Email Pipeline by Alec Meeker and Applequist Inc.
https://applequist.com
```

## Mobile Applications

### Splash Screen or About Section
Display during app launch or in settings/about:
```
Powered by Email Pipeline
by Alec Meeker and Applequist Inc.
```

## Embedded Systems / CLI Tools

### CLI Output Header
```
===================================================
Email Processor v1.0
Powered by Email Pipeline by Alec Meeker and Applequist Inc.
===================================================
```

## Acceptable Variations

The following variations are acceptable for space-constrained interfaces:

1. "Powered by Applequist Email Pipeline"
2. "Email Pipeline © Applequist Inc."
3. "Powered by Applequist Inc."

## Not Acceptable

The following would violate the license:

1. ❌ Hiding attribution in HTML comments
2. ❌ Using CSS `display: none` or `visibility: hidden`
3. ❌ Font size below 10px
4. ❌ Low contrast making text unreadable
5. ❌ Placing attribution only in source code
6. ❌ Removing attribution from commercial deployments

## Questions?

For clarification on attribution requirements or to request alternative attribution arrangements, contact Applequist Inc.