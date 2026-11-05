// Function to update the side panel HTML with specific disaster data
function updateSidebar(data) {
    document.getElementById('threat-name').innerText = data.primary_threat;
    document.getElementById('threat-location').innerText = data.location;
    document.getElementById('threat-time').innerText = data.time_of_event;
    document.getElementById('threat-severity').innerText = data.severity;
    
    // Check if probability exists (backward compatibility if an older JSON is loaded)
    const probElement = document.getElementById('threat-prob');
    if (probElement) {
        probElement.innerText = data.probability || "Calculating...";
    }
    
    // Change color based on severity
    const badge = document.getElementById('threat-severity');
    badge.style.color = data.severity.toLowerCase() === 'high' ? '#ff3333' : '#ffaa00';

    // Update cascading effects list
    const effectsList = document.getElementById('threat-effects');
    effectsList.innerHTML = '';
    
    if (data.cascading_effects && Array.isArray(data.cascading_effects)) {
        data.cascading_effects.forEach(effect => {
            let li = document.createElement('li');
            li.innerText = effect;
            effectsList.appendChild(li);
        });
    }
}

// Initialize the hyper-realistic 3D Globe with free, keyless Blue Marble textures
const world = Globe()
    (document.getElementById('globeViz'))
    .globeImageUrl('//unpkg.com/three-globe/example/img/earth-blue-marble.jpg')
    .bumpImageUrl('//unpkg.com/three-globe/example/img/earth-topology.png')
    
    // Add realistic atmospheric glow
    .showAtmosphere(true)
    .atmosphereColor('lightskyblue')
    .atmosphereAltitude(0.15)
    
    // Deep space background
    .backgroundColor('#000000')
    
    // Disaster point styling
    .pointAltitude(0.1)
    .pointRadius(2.0)
    
    // Hover text label
    .pointLabel('hoverText')
    
    // INTERACTIVITY: When a user clicks a point on the globe
    .onPointClick(point => {
        // Stop auto-rotation so the user can inspect
        world.controls().autoRotate = false;
        
        // Update the sidebar with the clicked disaster's data
        updateSidebar(point.fullData);
        
        // Smoothly fly the camera to the clicked disaster
        world.pointOfView({ lat: point.lat, lng: point.lng, altitude: 1.5 }, 1500);
        
        // Resume rotation after 10 seconds of inactivity
        setTimeout(() => { world.controls().autoRotate = true; }, 10000);
    });

// Auto-rotate the globe slowly like a real orbit
world.controls().autoRotate = true;
world.controls().autoRotateSpeed = 0.8;

// Array to store all threats so they show up at once
let globalThreats = [];
let lastThreatTime = ""; 

// Function to fetch real AI data from Flask
async function fetchRealLiveThreat() {
    try {
        const response = await fetch('/api/latest_threat');
        const data = await response.json();

        if (data.error) return; // Skip if no data yet

        // Only add to the globe if the AI has pushed a completely new prediction
        if (data.time_of_event !== lastThreatTime) {
            lastThreatTime = data.time_of_event;

            // Automatically show the newest threat in the sidebar
            updateSidebar(data);

            const severityColor = data.severity.toLowerCase() === 'high' ? '#ff3333' : '#ffaa00';

            // Store the FULL data payload inside the point object for the click handler
            globalThreats.push({
                lat: data.lat,
                lng: data.lon,
                size: 2,
                color: severityColor,
                hoverText: `${data.primary_threat} - ${data.location}`,
                fullData: data // <--- This holds the complete JSON for this specific event
            });
            
            // Plot all accumulated points
            world.pointsData(globalThreats)
                 .pointColor('color');
                 
            // Briefly fly to the newest threat as it arrives
            world.pointOfView({ lat: data.lat, lng: data.lon, altitude: 2 }, 2000);
        }
    } catch (error) {
        console.error("Error fetching live threat:", error);
    }
}

// Fetch immediately, then check for new AI data every 5 seconds
fetchRealLiveThreat();
setInterval(fetchRealLiveThreat, 5000);